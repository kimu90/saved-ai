import asyncio
import logging
import time
from typing import AsyncIterable, Optional, Dict
from .llm_manager import GeminiLLMManager
from ai_services_api.services.message.core.database import get_db_connection
import asyncio
from typing import Dict, Any, AsyncGenerator
from ai_services_api.services.chatbot.utils.db_utils import DatabaseConnector

logger = logging.getLogger(__name__)

class MessageHandler:
    def __init__(self, llm_manager):
        self.metadata = None
        self.llm_manager = llm_manager
    
    @staticmethod
    def clean_response_text(text: str) -> str:
        """
        Clean and format the response text to remove markdown formatting and make it more natural.
        Converts DOI citations into clickable links with publication titles.
        
        Args:
            text (str): The input text with markdown formatting
            
        Returns:
            str: Cleaned and reformatted text
        """
        # Replace common markdown patterns
        cleaned = text.replace('\n**', ' ')
        cleaned = cleaned.replace('**', '')
        
        # Remove numbered list formatting and bullet points
        import re
        cleaned = re.sub(r'\n\d+\.', '', cleaned)
        
        # Handle bullet points - replace with comma-separated list
        bullet_points = re.findall(r'\*\s*([^*\n]+)', cleaned)
        if bullet_points:
            # Remove all bullet points first
            cleaned = re.sub(r'\*\s*[^*\n]+\n*', '', cleaned)
            # Add them back as a flowing sentence
            bullet_list = ', '.join(point.strip() for point in bullet_points)
            if 'Key Findings:' in cleaned:
                cleaned = cleaned.replace('Key Findings:', f'Key findings include: {bullet_list}.')
        
        # Handle DOI citations - transform into clickable links
        # Look for patterns like "Study Title (DOI: https://doi.org/...)" or variations
        doi_patterns = [
            r'(.*?)\s*\(DOI:\s*https?://doi\.org/([^\)]+)\)',  # (DOI: https://doi.org/...)
            r'(.*?)\s*\(doi:\s*([^\)]+)\)',                    # (doi: ...)
            r'(.*?)\s*\(DOI:\s*([^\)]+)\)'                     # (DOI: ...)
        ]
        
        for pattern in doi_patterns:
            def replace_doi(match):
                title = match.group(1).strip()
                doi = match.group(2).strip()
                # Remove spaces in DOI URL
                doi = doi.replace(' ', '')
                return f'<a href="https://doi.org/{doi}">{title}</a>'
            
            cleaned = re.sub(pattern, replace_doi, cleaned)
        
        # Clean up quotation marks
        cleaned = re.sub(r'\"([^\"]+)\"', r'\1', cleaned)
        
        # Fix spacing issues
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = re.sub(r'\s+([.,:])', r'\1', cleaned)
        cleaned = cleaned.strip()
        
        # Add proper spacing after periods
        cleaned = re.sub(r'\.(?! )', '. ', cleaned)
        
        # Clean up any remaining special characters
        cleaned = cleaned.replace('\\n', ' ')
        cleaned = cleaned.strip()
        
        return cleaned

    async def process_stream_response(self, response_stream):
        """
        Process the streaming response and apply formatting.
        
        Args:
            response_stream: Async generator of response chunks
            
        Yields:
            str: Cleaned and formatted response chunks
        """
        buffer = ""
        async for chunk in response_stream:
            if isinstance(chunk, dict) and chunk.get('is_metadata'):
                continue
                
            if isinstance(chunk, dict) and 'chunk' in chunk:
                text = chunk['chunk']
            elif isinstance(chunk, (str, bytes)):
                text = chunk.decode('utf-8') if isinstance(chunk, bytes) else chunk
            else:
                continue
                
            buffer += text
            
            # Process complete sentences
            while '.' in buffer:
                split_idx = buffer.find('.') + 1
                sentence = self.clean_response_text(buffer[:split_idx])
                buffer = buffer[split_idx:].lstrip()
                
                if sentence.strip():
                    yield sentence
        
        # Handle remaining buffer
        if buffer.strip():
            yield self.clean_response_text(buffer)

    async def send_message_async(self, message: str, user_id: str, session_id: str = None):
        """
        Process message and handle responses with enhanced logging and formatting.
        
        Args:
            message (str): The message to be processed
            user_id (str): Unique identifier for the user
            session_id (str, optional): Unique identifier for the conversation session
        """
        start_time = time.time()
        logger.info("Starting async message processing")
        logger.debug(f"Message details - User ID: {user_id}, Session ID: {session_id}, Message length: {len(message)}")
        
        try:
            self.metadata = None
            logger.debug("Metadata reset before processing")
            
            # Get the response stream from LLM
            raw_response_stream = self.llm_manager.generate_async_response(message)
            
            # Initialize response tracking
            response_chunks = []
            
            # Process and format the response stream
            async for formatted_chunk in self.process_stream_response(raw_response_stream):
                logger.debug(f"Yielding formatted response chunk (length: {len(formatted_chunk)})")
                response_chunks.append(formatted_chunk)
                yield formatted_chunk
            
            # Prepare complete response
            complete_response = ''.join(response_chunks)
            response_time = time.time() - start_time
            
            logger.info("Message processing completed successfully")
            logger.debug(f"Total processing time: {response_time:.2f} seconds")
            
            # Save chat to database
            await self.save_chat_to_db(user_id, message, complete_response, response_time)
            
            # Get sentiment data from LLM
            sentiment_data = await self.llm_manager.analyze_sentiment(message)
            
            # Record interaction with sentiment metrics
            await self.record_interaction(session_id, user_id, message, {
                'response': complete_response,
                'metrics': {
                    'response_time': response_time,
                    'sentiment': sentiment_data
                }
            })
            
        except Exception as e:
            logger.error("Critical error in message stream processing", exc_info=True)
            logger.error(f"Error details - User ID: {user_id}, Session ID: {session_id}")
            
            if hasattr(e, '__traceback__'):
                tb = e.__traceback__
                logger.error(f"Error occurred in file: {tb.tb_frame.f_code.co_filename}, line: {tb.tb_lineno}")
            
            error_message = "I encountered an error processing your message. Please try again."
            logger.warning(f"Yielding error message to user: {error_message}")
            yield error_message
            
        finally:
            logger.info("Async message processing concluded")
            total_time = time.time() - start_time
            logger.debug(f"Total method execution time: {total_time:.2f} seconds")
    async def start_chat_session(self, user_id: str) -> str:
        """
        Start a new chat session.
        
        Args:
            user_id (str): Unique identifier for the user
        
        Returns:
            str: Generated session identifier
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Generate unique session identifier
                    session_id = f"session_{user_id}_{int(time.time())}"
                    
                    try:
                        cursor.execute("""
                            INSERT INTO chat_sessions 
                                (session_id, user_id, start_timestamp)
                            VALUES (%s, %s, CURRENT_TIMESTAMP)
                            RETURNING session_id
                        """, (session_id, user_id))
                        
                        conn.commit()
                        logger.info(f"Created chat session: {session_id}")
                        return session_id
                    
                    except Exception as insert_error:
                        conn.rollback()
                        logger.error(f"Error inserting chat session: {insert_error}")
                        raise
        
        except Exception as e:
            logger.error(f"Error in start_chat_session: {e}")
            raise

                
    async def save_chat_to_db(self, user_id: str, query: str, response: str, response_time: float):
        """Save chat interaction to database."""
        try:
            async with DatabaseConnector.get_connection() as conn:
                await conn.execute("""
                    INSERT INTO chatbot_logs 
                        (user_id, query, response, response_time, timestamp)
                    VALUES ($1, $2, $3, $4, NOW())
                """, user_id, query, response, response_time)
        except Exception as e:
            logger.error(f"Error saving chat to database: {e}")

    
    async def record_interaction(self, session_id: str, user_id: str, query: str, response_data: dict):
        try:
            async with DatabaseConnector.get_connection() as conn:
                # Start a transaction
                async with conn.transaction():
                    metrics = response_data.get('metrics', {})
                    
                    # Get the chat_log_id from the chatbot_logs table
                    chat_log_result = await conn.fetchrow("""
                        SELECT id FROM chatbot_logs 
                        WHERE user_id = $1 AND query = $2 
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, user_id, query)
                    
                    if not chat_log_result:
                        logger.warning("Corresponding chat log not found for sentiment metrics")
                        return
                    
                    chat_log_id = chat_log_result['id']
                    
                    # Get sentiment data from metrics
                    sentiment_data = metrics.get('sentiment', {})
                    emotion_labels = sentiment_data.get('emotion_labels', [])
                    
                    # Insert into sentiment_metrics table
                    await conn.execute("""
                        INSERT INTO sentiment_metrics
                            (chatbot_log_id, sentiment_score, emotion_labels,
                            satisfaction_score, urgency_score, clarity_score)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                        chat_log_id,
                        sentiment_data.get('sentiment_score', 0.0),
                        emotion_labels,
                        sentiment_data.get('aspects', {}).get('satisfaction', 0.0),
                        sentiment_data.get('aspects', {}).get('urgency', 0.0),
                        sentiment_data.get('aspects', {}).get('clarity', 0.0)
                    )
                    
                    logger.info(f"Recorded sentiment metrics for chat log ID: {chat_log_id}")
                    
        except Exception as e:
            logger.error(f"Error recording interaction and sentiment metrics: {e}", exc_info=True)
            raise

    async def update_session_stats(self, session_id: str, successful: bool = True):
        """Update session statistics with async database."""
        try:
            async with DatabaseConnector.get_connection() as conn:
                await conn.execute("""
                    UPDATE chat_sessions 
                    SET total_messages = total_messages + 1,
                        successful = $1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE session_id = $2
                """, successful, session_id)
                
                logger.info(f"Updated session stats for {session_id}")
                
        except Exception as e:
            logger.error(f"Error updating session stats: {e}", exc_info=True)
            raise

    async def _create_error_metadata(self, start_time: float, error_type: str) -> Dict:
        """Create standardized error metadata."""
        return {
            'metrics': {
                'response_time': time.time() - start_time,
                'intent': {'type': 'error', 'confidence': 0.0},
                'sentiment': {
                    'sentiment_score': 0.0,
                    'emotion_labels': ['error'],
                    'aspects': {
                        'satisfaction': 0.0,
                        'urgency': 0.0,
                        'clarity': 0.0
                    }
                },
                'content_matches': [],
                'content_types': {
                    'navigation': 0,
                    'publication': 0
                },
                'error_type': error_type
            },
            'error_occurred': True
        }

    async def update_content_click(self, interaction_id: int, content_id: str):
        """
        Update when a user clicks on a content match.
        
        Args:
            interaction_id (int): Interaction identifier
            content_id (str): Clicked content identifier
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute("""
                            UPDATE chat_analytics 
                            SET clicked = true 
                            WHERE interaction_id = %s AND content_id = %s
                        """, (interaction_id, content_id))
                        conn.commit()
                    except Exception as update_error:
                        conn.rollback()
                        logger.error(f"Error updating content click: {update_error}")
                        raise
        except Exception as e:
            logger.error(f"Error in update_content_click: {e}")
            raise
            
    async def flush_conversation_cache(self, conversation_id: str):
        """Clears the conversation history stored in the memory."""
        try:
            memory = self.llm_manager.create_memory()
            memory.clear()
            logger.info(f"Successfully flushed conversation cache for ID: {conversation_id}")
        except Exception as e:
            logger.error(f"Error while flushing conversation cache for ID {conversation_id}: {e}")
            raise RuntimeError(f"Failed to clear conversation history: {str(e)}")
