import os
import logging
import google.generativeai as genai
from typing import Optional, List, Dict, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
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

    def classify_field_and_subfield(self, title: str, abstract: str, domains: List[str]) -> Tuple[str, str]:
        """
        Dynamically classify content by analyzing its themes and patterns.
        
        Args:
            title: Content title
            abstract: Content abstract
            domains: List of domain tags
            
        Returns:
            Tuple[str, str]: (field, subfield)
        """
        prompt = f"""
        Analyze this academic content and create a natural field classification:

        Title: {title}
        Abstract: {abstract}
        Domains: {', '.join(domains)}

        Instructions:
        1. First, determine the broad field this content belongs to, considering the overall theme and academic discipline.
        2. Then, determine a more specific subfield that best describes the specialized area within that field.
        3. The classification should emerge naturally from the content rather than fitting into predefined categories.
        4. Your field should be broad enough to group similar content but specific enough to be meaningful.
        5. Your subfield should capture the specific focus area within that field.

        Return ONLY:
        FIELD: [naturally derived field]
        SUBFIELD: [specific subfield within that field]

        For example:
        For a paper about machine learning in agriculture:
        FIELD: Agricultural Technology
        SUBFIELD: AI-Driven Crop Management
        """

        try:
            response = self.model.generate_content(prompt)
            result = response.text.strip()
            
            field = None
            subfield = None
            
            for line in result.split('\n'):
                if line.startswith('FIELD:'):
                    field = line.replace('FIELD:', '').strip()
                elif line.startswith('SUBFIELD:'):
                    subfield = line.replace('SUBFIELD:', '').strip()
            
            if field and subfield:
                return field, subfield
            else:
                # Only use this as a last resort if classification fails
                return "Unclassified", "General"
                
        except Exception as e:
            logger.error(f"Error in field classification: {e}")
            return "Unclassified", "General"
        
    def analyze_content_corpus(self, publications: List[Dict]) -> Dict[str, List[str]]:
        """
        Analyze a corpus of publications to identify natural field groupings.
        
        Args:
            publications: List of publication dictionaries with titles, abstracts, etc.
        
        Returns:
            Dict mapping identified fields to lists of common subfields
        """
        corpus_prompt = f"""
        Analyze this collection of {len(publications)} academic publications and identify natural groupings:

        Publications:
        {json.dumps([{
            'title': p.get('title', ''),
            'abstract': p.get('abstract', '')[:200],  # Truncate for prompt length
            'domains': p.get('domains', [])
        } for p in publications[:50]], indent=2)}  # Sample for analysis

        Task:
        1. Identify the major thematic fields that emerge from this content
        2. For each field, identify common specialized subfields
        3. Consider interdisciplinary areas and emerging fields
        4. Group similar themes while maintaining meaningful distinctions

        Return the classification structure as:
        FIELDS:
        [Field 1]
        - [Subfield 1.1]
        - [Subfield 1.2]
        [Field 2]
        - [Subfield 2.1]
        - [Subfield 2.2]
        ...etc.
        """

        try:
            response = self.model.generate_content(corpus_prompt)
            # Parse the response into a structured format
            fields = {}
            current_field = None
            
            for line in response.text.strip().split('\n'):
                if line.startswith('-'):
                    if current_field:
                        fields[current_field].append(line.replace('-', '').strip())
                else:
                    current_field = line.strip()
                    if current_field and not current_field.startswith('FIELDS:'):
                        fields[current_field] = []
                        
            return fields
            
        except Exception as e:
            logger.error(f"Error analyzing content corpus: {e}")
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