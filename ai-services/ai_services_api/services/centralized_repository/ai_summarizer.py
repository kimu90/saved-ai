import os
import logging
import google.generativeai as genai
from typing import Optional, List, Dict, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
from ai_services_api.services.centralized_repository.database_manager import DatabaseManager

import json
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class TextSummarizer:
    def __init__(self):
        """Initialize the TextSummarizer with Gemini model."""
        self.model = self._setup_gemini()
        self.content_types = ["articles", "publications", "blogs", "multimedia"]
        
        # Import DatabaseManager here
        from ai_services_api.services.centralized_repository.database_manager import DatabaseManager
        
        # Create a database manager instance
        self.db = DatabaseManager()
        
        # Property to store the dynamically generated fields and their subfields
        # Will be populated after corpus analysis
        self.corpus_fields = {}
        self.has_analyzed_corpus = False
        
        logger.info("TextSummarizer initialized successfully")
    def _setup_gemini(self):
        """Set up and configure the Gemini model."""
        try:
            api_key = os.getenv('GEMINI_API_KEY')
            if not api_key:
                raise ValueError("GEMINI_API_KEY environment variable is not set")
            
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-pro')
            logger.info("Gemini model setup completed")
            return model
            
        except Exception as e:
            logger.error(f"Error setting up Gemini model: {e}")
            raise

    @retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def summarize(self, title: str, abstract: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Generate a summary of the title and abstract using Gemini and classify the content type.
        
        Args:
            title: Title of the content
            abstract: Abstract or content body
            
        Returns:
            Tuple[str, str]: (Generated summary, Content type classification)
        """
        try:
            if not title:
                logger.error("Title is required for summarization")
                return ("Cannot generate summary: title is missing", None)

            # Always use the combined prompt
            if not abstract or abstract.strip() == "N/A":
                abstract = title  # Use title as content if no abstract available
                
            prompt = self._create_combined_prompt(title, abstract)
            
            # Generate summary and classification
            response = self.model.generate_content(prompt)
            result = response.text.strip()
            
            if not result:
                logger.warning("Generated content is empty")
                return ("Failed to generate meaningful content", None)
            
            # Parse the response to separate summary and content type
            summary, content_type = self._parse_response(result)
            
            # Clean and format summary
            cleaned_summary = self._clean_summary(summary)
            logger.info(f"Successfully generated content for: {title[:100]}...")
            
            return (cleaned_summary, content_type)

        except Exception as e:
            logger.error(f"Error in content generation: {e}")
            return ("Failed to generate content due to technical issues", None)

    
        
    def analyze_content_corpus(self, publications: List[Dict]) -> Dict[str, List[str]]:
        """
        Comprehensive corpus analysis for publication classification.
        """
        # First, check existing fields in the database
        existing_fields = self._check_existing_fields_in_database()
        
        if existing_fields:
            # If existing fields are found, use them
            logger.info(f"Using existing fields: {list(existing_fields.keys())}")
            return existing_fields
        
        # Prepare publications data for analysis
        publication_data = [{
            'title': p.get('title', ''),
            'abstract': p.get('abstract', '')[:500],
            'domains': p.get('domains', []),
            'keywords': p.get('keywords', [])
        } for p in publications]
        
        # Prompt for generating comprehensive fields with explicit 10-field constraint
        corpus_prompt = f"""
        Critically analyze {len(publications)} academic publications and generate a STRICT 10-field classification system.

        ABSOLUTE REQUIREMENTS:
        1. GENERATE EXACTLY 10 FIELDS - NO MORE, NO LESS
        2. Each field must have PRECISELY 4 subfields
        3. Comprehensively cover ALL publications
        4. Ensure mutually exclusive yet exhaustive categorization

        Constraints for Classification:
        - Cover entire research spectrum
        - Capture interdisciplinary nuances
        - Represent diverse academic domains

        Sample Publication Context:
        {json.dumps([{
            'title': p.get('title', '')[:100], 
            'domains': p.get('domains', [])
        } for p in publication_data[:10]], indent=2)}

        OUTPUT FORMAT (STRICT):
        FIELDS:
        [Field 1]
        - [Subfield 1.1]
        - [Subfield 1.2]
        - [Subfield 1.3]
        - [Subfield 1.4]
        [Field 2]
        - [Subfield 2.1]
        - [Subfield 2.2]
        - [Subfield 2.3]
        - [Subfield 2.4]
        ... (continue EXACTLY for 10 fields)
        """
        
        try:
            # Generate classification using Gemini
            response = self.model.generate_content(corpus_prompt)
            fields = {}
            current_field = None
            
            # Parse the response
            for line in response.text.strip().split('\n'):
                if line.startswith('-'):
                    if current_field:
                        subfield = line.replace('-', '').strip()
                        if current_field in fields:
                            fields[current_field].append(subfield)
                        else:
                            fields[current_field] = [subfield]
                else:
                    potential_field = line.strip()
                    if potential_field and not potential_field.startswith('FIELDS:'):
                        current_field = potential_field
                        if current_field not in fields:
                            fields[current_field] = []
            
            # Strict validation
            if len(fields) != 10:
                # If more than 10 fields, take first 10
                if len(fields) > 10:
                    fields = dict(list(fields.items())[:10])
                else:
                    raise ValueError(f"Expected 10 fields, got {len(fields)}")
            
            # Ensure exactly 4 subfields per field
            for field, subfields in fields.items():
                if len(subfields) > 4:
                    fields[field] = subfields[:4]
                elif len(subfields) < 4:
                    # Pad with generic subfields if needed
                    while len(fields[field]) < 4:
                        fields[field].append(f"Generic {len(fields[field])+1} Subfield")
            
            # Store and return fields
            self.corpus_fields = fields
            self.has_analyzed_corpus = True
            
            logger.info(f"Comprehensive corpus analysis complete. Fields: {', '.join(fields.keys())}")
            
            return fields
        
        except Exception as e:
            logger.error(f"Error in comprehensive corpus analysis: {e}")
            raise

    def _check_existing_fields_in_database(self) -> Dict[str, List[str]]:
        """
        Check if fields and subfields already exist in the database.
        
        Returns:
            Dict of existing fields and their subfields, or empty dict if none found
        """
        try:
            # Query to get distinct fields and their associated subfields
            query = """
            SELECT DISTINCT field, subfield 
            FROM resources_resource 
            WHERE field IS NOT NULL AND subfield IS NOT NULL
            """
            
            # Use DatabaseManager to execute query
            results = self.db.execute(query)
            
            if not results:
                return {}
            
            # Organize results into a structured dictionary
            fields = {}
            for row in results:
                field, subfield = row
                if field not in fields:
                    fields[field] = []
                if subfield not in fields[field]:
                    fields[field].append(subfield)
            
            return fields
        
        except Exception as e:
            logger.error(f"Error checking existing fields in database: {e}")
            return {}

    def _create_combined_prompt(self, title: str, abstract: str) -> str:
        """Create a prompt for both summarization and content type classification."""
        return f"""
        Please analyze the following content and provide:
        1. A concise summary
        2. Classification of the content type (strictly choose one: articles, publications, blogs, multimedia)
        
        Title: {title}
        
        Content: {abstract}
        
        Instructions:
        1. Provide a clear and concise summary in 2-3 sentences
        2. Focus on the main points and implications
        3. Use appropriate language for the content type
        4. Keep the summary under 200 words
        5. Retain technical terms and key concepts
        6. Begin directly with the summary, do not include phrases like "This paper" or "This content"
        7. After the summary, on a new line, write "CONTENT_TYPE:" followed by one of: articles, publications, blogs, multimedia
        
        Example format:
        [Your summary here]
        CONTENT_TYPE: publications
        """

    def _create_title_only_prompt(self, title: str) -> str:
        """Create a prompt for generating a brief description and assigning a content genre dynamically with a limit of 10 genres."""

        return f"""
        Please analyze the following title and determine:
        1. A brief description of the content.
        2. A suitable **genre tag** (maximum of two words) that best represents the subject matter, selected from a predefined list.

        **Title:** {title}

        **Instructions:**
        - Provide a concise, single-sentence summary of what this content likely discusses.
        - Use phrases like "This content appears to discuss..." or "This work likely explores..."
        - Keep the description under 50 words.
        - Assign a **genre tag** (strictly one or two words) that best fits the content, choosing from the following:
        **"Reproductive Health," "Public Health," "Education," "Policy," "Research," "Nutrition," "Urbanization," "Gender Equity," "Climate Change," "Demography"**.
        - If the title does not clearly fit one of these, return "Uncategorized."

        **Example format:**
        [Your description here]  
        GENRE: [One of the 10 predefined genre tags]
        """


    def _parse_response(self, response: str) -> Tuple[str, Optional[str]]:
        """Parse the response to separate summary and content type."""
        try:
            parts = response.split('CONTENT_TYPE:', 1)
            summary = parts[0].strip()
            
            content_type = None
            if len(parts) > 1:
                content_type = parts[1].strip().lower()
                if content_type not in self.content_types:
                    logger.warning(f"Invalid content type detected: {content_type}")
                    content_type = None
            
            return summary, content_type
            
        except Exception as e:
            logger.error(f"Error parsing response: {e}")
            return response, None

    def _clean_summary(self, summary: str) -> str:
        """Clean and format the generated summary."""
        try:
            # Basic cleaning
            cleaned = summary.strip()
            cleaned = ' '.join(cleaned.split())  # Normalize whitespace
            
            # Remove common prefixes if present
            prefixes = [
                'Summary:', 
                'Here is a summary:', 
                'The summary is:', 
                'Here is a concise summary:',
                'This paper',
                'This research',
                'This study'
            ]
            
            lower_cleaned = cleaned.lower()
            for prefix in prefixes:
                if lower_cleaned.startswith(prefix.lower()):
                    cleaned = cleaned[len(prefix):].strip()
                    break
            
            # Ensure the summary starts with a capital letter
            if cleaned:
                cleaned = cleaned[0].upper() + cleaned[1:]
            
            # Add a period at the end if missing
            if cleaned and cleaned[-1] not in ['.', '!', '?']:
                cleaned += '.'
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Error cleaning summary: {e}")
            return summary

    def __del__(self):
        """Cleanup any resources."""
        try:
            # Add any cleanup code if needed
            pass
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")