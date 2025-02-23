from enum import Enum
import asyncio
import json
import logging
import re
import os
import time
from typing import Dict, Tuple, Any, List, AsyncGenerator
from datetime import datetime
from langchain.schema.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.callbacks import AsyncIteratorCallbackHandler

logger = logging.getLogger(__name__)

class QueryIntent(Enum):
    """Enum for different types of query intents."""
    NAVIGATION = "navigation"
    PUBLICATION = "publication"
    GENERAL = "general"

class CustomAsyncCallbackHandler(AsyncIteratorCallbackHandler):
    """Custom callback handler for streaming responses."""
    
    async def on_llm_start(self, *args, **kwargs):
        """Handle LLM start."""
        pass

    async def on_llm_new_token(self, token: str, *args, **kwargs):
        """Handle new token."""
        if token:
            self.queue.put_nowait(token)

    async def on_llm_end(self, *args, **kwargs):
        """Handle LLM end."""
        self.queue.put_nowait(None)

    async def on_llm_error(self, error: Exception, *args, **kwargs):
        """Handle LLM error."""
        self.queue.put_nowait(f"Error: {str(error)}")

class GeminiLLMManager:
    def __init__(self):
        """Initialize the LLM manager with required components."""
        try:
            # Load API key
            self.api_key = os.getenv("GEMINI_API_KEY")
            if not self.api_key:
                raise ValueError("GEMINI_API_KEY environment variable not set")

            # Initialize callback handler
            self.callback = CustomAsyncCallbackHandler()
            
            # Initialize context management
            self.context_window = []
            self.max_context_items = 5
            self.context_expiry = 1800  # 30 minutes
            self.confidence_threshold = 0.6
            
            # Initialize intent patterns
            self.intent_patterns = {
                QueryIntent.NAVIGATION: {
                    'patterns': [
                        (r'website', 1.0),
                        (r'page', 0.9),
                        (r'find', 0.8),
                        (r'where', 0.8),
                        (r'how to', 0.7),
                        (r'navigate', 0.9),
                        (r'section', 0.8),
                        (r'content', 0.7),
                        (r'information about', 0.7)
                    ],
                    'threshold': 0.6
                },
                QueryIntent.PUBLICATION: {
                    'patterns': [
                        (r'research', 1.0),
                        (r'paper', 1.0),
                        (r'publication', 1.0),
                        (r'study', 0.9),
                        (r'article', 0.9),
                        (r'journal', 0.8),
                        (r'doi', 0.9),
                        (r'published', 0.8),
                        (r'authors', 0.8),
                        (r'findings', 0.7)
                    ],
                    'threshold': 0.6
                }
            }
            
            logger.info("GeminiLLMManager initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing GeminiLLMManager: {e}", exc_info=True)
            raise

    def get_gemini_model(self):
        """Initialize and return the Gemini model."""
        return ChatGoogleGenerativeAI(
            google_api_key=self.api_key,
            stream=True,
            model="gemini-pro",
            convert_system_message_to_human=True,
            callbacks=[self.callback],
            temperature=0.7,
            top_p=0.9,
            top_k=40,
        )

    async def analyze_sentiment(self, message: str) -> Dict:
        """Analyze sentiment of a message using Gemini model."""
        try:
            prompt = """Analyze the sentiment of this message and return a JSON object. 
            Return ONLY the JSON object with no markdown formatting, no code blocks, and no additional text.
            
            Required format:
            {
                "sentiment_score": <float between -1 and 1>,
                "emotion_labels": [<array of emotion strings>],
                "confidence": <float between 0 and 1>,
                "aspects": {
                    "satisfaction": <float between 0 and 1>,
                    "urgency": <float between 0 and 1>,
                    "clarity": <float between 0 and 1>
                }
            }
            Message to analyze: """ + message
            
            response = await self.get_gemini_model().ainvoke(prompt)
            cleaned_response = response.content.strip()
            cleaned_response = cleaned_response.replace('```json', '').replace('```', '').strip()
            
            try:
                sentiment_data = json.loads(cleaned_response)
                logger.info(f"Sentiment analysis result: {sentiment_data}")
                return sentiment_data
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse sentiment analysis response: {cleaned_response}")
                logger.error(f"JSON parse error: {e}")
                return self._get_default_sentiment()
                
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {e}")
            return self._get_default_sentiment()

    def _get_default_sentiment(self) -> Dict:
        """Return default sentiment values."""
        return {
            'sentiment_score': 0.0,
            'emotion_labels': [],
            'confidence': 0.0,
            'aspects': {
                'satisfaction': 0.0,
                'urgency': 0.0,
                'clarity': 0.0
            }
        }

    async def detect_intent(self, message: str) -> Tuple[QueryIntent, float]:
        """Detect intent of the message with confidence scoring."""
        try:
            message = message.lower()
            intent_scores = {intent: 0.0 for intent in QueryIntent}
            
            for intent, config in self.intent_patterns.items():
                score = 0.0
                matches = 0
                
                for pattern, weight in config['patterns']:
                    if re.search(pattern, message):
                        score += weight
                        matches += 1
                
                if matches > 0:
                    intent_scores[intent] = score / matches
            
            max_intent = max(intent_scores.items(), key=lambda x: x[1])
            
            if max_intent[1] >= self.intent_patterns.get(max_intent[0], {}).get('threshold', 0.6):
                return max_intent[0], max_intent[1]
            
            return QueryIntent.GENERAL, 0.0
            
        except Exception as e:
            logger.error(f"Error in intent detection: {e}", exc_info=True)
            return QueryIntent.GENERAL, 0.0

    def _create_system_message(self, intent: QueryIntent) -> str:
        """
        Create appropriate system message based on intent, encouraging natural conversation flow.
        """
        base_message = (
            "You are a knowledgeable representative of APHRC (African Population and Health Research Center). "
            "Respond in a natural, conversational tone that flows well. Avoid using bullet points, "
            "numbered lists, or excessive formatting. Instead, present information in clear, "
            "well-structured paragraphs. When citing publications, integrate the citations "
            "smoothly into your sentences. "
        )
        
        if intent == QueryIntent.NAVIGATION:
            return base_message + (
                "Guide users through APHRC's website content as if you were giving a personal tour. "
                "Weave URLs naturally into your explanations, and describe website sections in a "
                "flowing narrative rather than a list. Make connections between different sections "
                "to help users understand how they relate to each other."
            )
        
        elif intent == QueryIntent.PUBLICATION:
            return base_message + (
                "Discuss APHRC's research publications as if you're having an engaging conversation "
                "about our findings. Present research summaries in a narrative style, integrating "
                "citations and DOIs smoothly into your discussion. Connect different research "
                "findings to tell a cohesive story about our work. When sharing key findings, "
                "present them as part of a flowing discussion rather than a list."
            )
        
        else:
            return base_message + (
                "Provide a comprehensive overview of APHRC's work by weaving together information "
                "about our website resources and research publications. Create a narrative that "
                "helps users understand both where to find information and what insights our "
                "research has uncovered. Make natural connections between different aspects of "
                "our work to provide a complete picture."
            )

    def create_context(self, relevant_data: List[Dict]) -> str:
        """
        Create a flowing context narrative from relevant content.
        """
        if not relevant_data:
            return ""
        
        navigation_content = []
        publication_content = []
        
        for item in relevant_data:
            text = item.get('text', '')
            metadata = item.get('metadata', {})
            content_type = metadata.get('type', 'unknown')
            
            if content_type == 'navigation':
                navigation_content.append(
                    f"The {metadata.get('title', 'section')} of our website ({metadata.get('url', '')}) "
                    f"provides information about {text[:300].strip()}..."
                )
            
            elif content_type == 'publication':
                authors = metadata.get('authors', 'our researchers')
                date = metadata.get('date', '')
                date_text = f" in {date}" if date else ""
                
                publication_content.append(
                    f"In a study published{date_text}, {authors} explored {metadata.get('title', 'research')}. "
                    f"Their work revealed that {text[:300].strip()}..."
                )
        
        context_parts = []
        
        if navigation_content:
            context_parts.append(
                "Regarding our online resources: " + 
                " ".join(navigation_content)
            )
        
        if publication_content:
            context_parts.append(
                "Our research has produced several relevant findings: " + 
                " ".join(publication_content)
            )
        
        return "\n\n".join(context_parts)

    def manage_context_window(self, new_context: Dict):
        """Manage sliding window of conversation context."""
        current_time = datetime.now().timestamp()
        
        # Remove expired contexts
        self.context_window = [
            ctx for ctx in self.context_window 
            if current_time - ctx.get('timestamp', 0) < self.context_expiry
        ]
        
        # Add new context
        new_context['timestamp'] = current_time
        self.context_window.append(new_context)
        
        # Maintain maximum window size
        if len(self.context_window) > self.max_context_items:
            self.context_window.pop(0)

    async def generate_async_response(self, message: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate async response with parallel processing and enhanced logging."""
        start_time = time.time()
        logger.info(f"Starting async response generation for message: {message}")
        
        try:
            # Log message preprocessing
            logger.debug("Preprocessing message")
            processed_message = message
            
            # Log parallel task creation
            logger.info("Creating parallel processing tasks")
            try:
                intent_task = asyncio.create_task(self.detect_intent(processed_message))
                sentiment_task = asyncio.create_task(self.analyze_sentiment(processed_message))
                
                # Log task waiting
                logger.debug("Waiting for intent and sentiment tasks to complete")
                intent_result, sentiment_data = await asyncio.gather(intent_task, sentiment_task)
            except Exception as task_error:
                logger.error(f"Error in parallel task processing: {task_error}", exc_info=True)
                raise
            
            # Log intent and sentiment results
            logger.info(f"Intent detected: {intent_result[0]} (Confidence: {intent_result[1]})")
            logger.debug(f"Sentiment analysis result: {sentiment_data}")
            
            # Unpack intent result
            intent, confidence = intent_result
            
            # Create context and manage window
            context = "I'll help you find information about APHRC's publications."
            logger.debug("Managing context window")
            self.manage_context_window({'text': context, 'query': processed_message})
            
            # Prepare messages for model
            logger.debug("Preparing system and human messages")
            system_message = self._create_system_message(intent)
            messages = [
                SystemMessage(content=system_message),
                HumanMessage(content=f"Context: {context}\n\nQuery: {processed_message}")
            ]
            
            # Initialize response tracking
            response_chunks = []
            buffer = ""
            
            try:
                logger.info("Initializing model streaming")
                model = self.get_gemini_model()
                async for response in model.astream(messages):
                    if not response.content:
                        logger.debug("Skipping empty response content")
                        continue
                        
                    buffer += response.content
                    logger.debug(f"Received response chunk. Current buffer length: {len(buffer)}")
                    
                    # Yield chunks based on complete sentences or size
                    while '.' in buffer or len(buffer) > 100:
                        split_idx = buffer.find('.') + 1 if '.' in buffer else len(buffer)
                        chunk = buffer[:split_idx]
                        buffer = buffer[split_idx:].lstrip()
                        
                        if chunk.strip():
                            response_chunks.append(chunk)
                            logger.debug(f"Yielding chunk (length: {len(chunk)})")
                            yield {
                                'chunk': chunk,
                                'is_metadata': False
                            }
                
                # Handle any remaining buffer
                if buffer.strip():
                    logger.debug(f"Yielding final buffer chunk (length: {len(buffer)})")
                    response_chunks.append(buffer)
                    yield {
                        'chunk': buffer,
                        'is_metadata': False
                    }
                
                # Prepare final response
                complete_response = ''.join(response_chunks)
                logger.info(f"Complete response generated. Total length: {len(complete_response)}")
                
                # Yield metadata
                logger.debug("Preparing and yielding metadata")
                yield {
                    'is_metadata': True,
                    'metadata': {
                        'response': complete_response,
                        'timestamp': datetime.now().isoformat(),
                        'metrics': {
                            'response_time': time.time() - start_time,
                            'intent': {
                                'type': intent.value,
                                'confidence': confidence
                            },
                            'sentiment': sentiment_data
                        },
                        'error_occurred': False
                    }
                }
                
            except Exception as stream_error:
                logger.error(f"Error in stream processing: {stream_error}", exc_info=True)
                error_message = "I apologize, but I encountered an error processing your request. Please try again."
                yield {
                    'chunk': error_message,
                    'is_metadata': False
                }
                
        except Exception as e:
            logger.error(f"Critical error generating response: {e}", exc_info=True)
            error_message = "I apologize, but I encountered an error. Please try again."
            
            # Yield error chunk
            yield {
                'chunk': error_message,
                'is_metadata': False
            }
            
            # Yield error metadata
            yield {
                'is_metadata': True,
                'metadata': {
                    'response': error_message,
                    'timestamp': datetime.now().isoformat(),
                    'metrics': {
                        'response_time': time.time() - start_time,
                        'intent': {'type': 'error', 'confidence': 0.0},
                        'sentiment': self._get_default_sentiment()
                    },
                    'error_occurred': True
                }
            }
        
        logger.info(f"Async response generation completed. Total time: {time.time() - start_time:.2f} seconds")