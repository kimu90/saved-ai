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

            if not abstract or abstract.strip() == "N/A":
                logger.info("No abstract available, generating description from title")
                prompt = self._create_title_only_prompt(title)
            else:
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
        Classify content into predefined fields and subfields using Gemini.
        
        Args:
            title: Content title
            abstract: Content abstract
            domains: List of domain tags
            
        Returns:
            Tuple[str, str]: (field, subfield)
        """
        field_hierarchy = {
            "Computer Science": ["AI/ML", "Software Engineering", "Data Science", "Cybersecurity", "Networks"],
            "Life Sciences": ["Molecular Biology", "Genetics", "Neuroscience", "Ecology", "Biotechnology"],
            "Physical Sciences": ["Physics", "Chemistry", "Astronomy", "Materials Science", "Earth Science"],
            "Engineering": ["Mechanical", "Electrical", "Civil", "Chemical", "Aerospace"],
            "Medicine": ["Clinical Research", "Public Health", "Pharmacology", "Medical Technology", "Epidemiology"],
            "Social Sciences": ["Psychology", "Sociology", "Economics", "Political Science", "Anthropology"],
            "Environmental Science": ["Climate Studies", "Conservation", "Environmental Policy", "Sustainability", "Resource Management"],
            "Mathematics": ["Pure Mathematics", "Applied Mathematics", "Statistics", "Operations Research", "Mathematical Physics"],
            "Business": ["Management", "Finance", "Marketing", "Operations", "Entrepreneurship"],
            "Humanities": ["History", "Philosophy", "Literature", "Cultural Studies", "Linguistics"]
        }

        prompt = f"""
        Analyze this content and classify it into exactly one field and one subfield:

        Title: {title}
        Abstract: {abstract}
        Domains: {', '.join(domains)}

        Available Fields and Subfields:
        {json.dumps(field_hierarchy, indent=2)}

        Return ONLY:
        FIELD: [main field]
        SUBFIELD: [specific subfield]
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
            
            if field in field_hierarchy and subfield in field_hierarchy[field]:
                return field, subfield
            else:
                return "Computer Science", "Software Engineering"  # Default fallback
                
        except Exception as e:
            logger.error(f"Error in field classification: {e}")
            return "Computer Science", "Software Engineering"

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