"""
Graph database initialization and semantic processing module.
"""
import os
import logging
import psycopg2
import google.generativeai as genai
from neo4j import GraphDatabase
from typing import List, Dict, Any, Optional
import json
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure Gemini
genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
model = genai.GenerativeModel('gemini-pro')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DatabaseConnectionManager:
    """Manages database connections and configuration"""
    
    @staticmethod
    def get_postgres_connection():
        """Create a connection to PostgreSQL database."""
        database_url = os.getenv('DATABASE_URL')
        
        if database_url:
            parsed_url = urlparse(database_url)
            conn_params = {
                'host': parsed_url.hostname,
                'port': parsed_url.port,
                'dbname': parsed_url.path[1:],
                'user': parsed_url.username,
                'password': parsed_url.password
            }
        else:
            conn_params = {
                'host': os.getenv('POSTGRES_HOST', 'localhost'),
                'port': os.getenv('POSTGRES_PORT', '5432'),
                'dbname': os.getenv('POSTGRES_DB', 'aphrc'),
                'user': os.getenv('POSTGRES_USER', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD', 'p0stgres')
            }

        try:
            conn = psycopg2.connect(**conn_params)
            logger.info(f"Successfully connected to database: {conn_params['dbname']}")
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f"Error connecting to the database: {e}")
            raise

class GraphDatabaseInitializer:
    def __init__(self):
        """Initialize GraphDatabaseInitializer."""
        self._neo4j_driver = GraphDatabase.driver(
            os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
            auth=(
                os.getenv('NEO4J_USER', 'neo4j'),
                os.getenv('NEO4J_PASSWORD')
            )
        )
        logger.info("Neo4j driver initialized")

    def _create_indexes(self):
        """Create enhanced indexes in Neo4j"""
        index_queries = [
            # Basic indexes
            "CREATE INDEX expert_id IF NOT EXISTS FOR (e:Expert) ON (e.id)",
            "CREATE INDEX expert_name IF NOT EXISTS FOR (e:Expert) ON (e.name)",
            "CREATE INDEX expert_orcid IF NOT EXISTS FOR (e:Expert) ON (e.orcid)",
            "CREATE INDEX theme_name IF NOT EXISTS FOR (t:Theme) ON (t.name)",
            "CREATE INDEX unit_name IF NOT EXISTS FOR (u:Unit) ON (u.name)",
            
            # New indexes for fields and domains
            "CREATE INDEX field_name IF NOT EXISTS FOR (f:Field) ON (f.name)",
            "CREATE INDEX domain_name IF NOT EXISTS FOR (d:Domain) ON (d.name)",
            
            # Semantic indexes
            "CREATE INDEX concept_name IF NOT EXISTS FOR (c:Concept) ON (c.name)",
            "CREATE INDEX area_name IF NOT EXISTS FOR (ra:ResearchArea) ON (ra.name)",
            "CREATE INDEX method_name IF NOT EXISTS FOR (m:Method) ON (m.name)",
            "CREATE INDEX related_name IF NOT EXISTS FOR (r:RelatedArea) ON (r.name)",
            
            # Fulltext indexes
            """CREATE FULLTEXT INDEX expert_fulltext IF NOT EXISTS 
               FOR (e:Expert) ON EACH [e.name, e.designation]""",
            """CREATE FULLTEXT INDEX concept_fulltext IF NOT EXISTS 
               FOR (c:Concept) ON EACH [c.name]"""
        ]
        
        with self._neo4j_driver.session() as session:
            for query in index_queries:
                try:
                    session.run(query)
                    logger.info(f"Index created: {query}")
                except Exception as e:
                    logger.warning(f"Error creating index: {e}")

    def _fetch_experts_data(self):
        """Fetch experts data from PostgreSQL"""
        conn = None
        try:
            conn = DatabaseConnectionManager.get_postgres_connection()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT 
                    id,
                    first_name, 
                    last_name,
                    knowledge_expertise,
                    designation,
                    domains,
                    fields,
                    theme,
                    unit,
                    orcid,
                    is_active
                FROM experts_expert
                WHERE id IS NOT NULL
            """)
            
            experts_data = cur.fetchall()
            logger.info(f"Fetched {len(experts_data)} experts from database")
            return experts_data
        except Exception as e:
            logger.error(f"Error fetching experts data: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def _process_historical_data(self):
        """Process existing search matches and messages for adaptive patterns"""
        conn = None
        try:
            conn = DatabaseConnectionManager.get_postgres_connection()
            cur = conn.cursor()
            
            # Get experts who appear together in search results
            cur.execute("""
                SELECT 
                    e1.expert_id as expert1_id,
                    e2.expert_id as expert2_id,
                    COUNT(*) as co_occurrence,
                    AVG(e1.similarity_score + e2.similarity_score) / 2 as avg_similarity
                FROM expert_search_matches e1
                JOIN expert_search_matches e2 
                    ON e1.search_id = e2.search_id 
                    AND e1.expert_id < e2.expert_id
                GROUP BY e1.expert_id, e2.expert_id
                HAVING COUNT(*) > 1
            """)
            search_patterns = cur.fetchall()
            
            # Get direct communication patterns
            cur.execute("""
                SELECT 
                    sender_id,
                    receiver_id,
                    COUNT(*) as message_count,
                    MAX(created_at) as last_interaction
                FROM expert_messages
                GROUP BY sender_id, receiver_id
                HAVING COUNT(*) > 0
            """)
            message_patterns = cur.fetchall()
            
            return search_patterns, message_patterns
            
        except Exception as e:
            logger.error(f"Error processing historical data: {e}")
            return [], []
        finally:
            if conn:
                conn.close()

    def _create_adaptive_relationships(self, session, search_patterns, message_patterns):
        """Create/update relationship weights based on actual usage"""
        try:
            # Add weights from search co-occurrence
            for expert1_id, expert2_id, co_occurrence, avg_similarity in search_patterns:
                weight = min(co_occurrence * 0.1 * avg_similarity, 1.0)
                session.run("""
                    MATCH (e1:Expert {id: $expert1_id})
                    MATCH (e2:Expert {id: $expert2_id})
                    MERGE (e1)-[r:FREQUENTLY_SEARCHED_WITH]-(e2)
                    SET r.weight = $weight,
                        r.co_occurrence = $co_occurrence,
                        r.last_updated = datetime()
                """, {
                    "expert1_id": str(expert1_id),
                    "expert2_id": str(expert2_id),
                    "weight": weight,
                    "co_occurrence": co_occurrence
                })

            # Add weights from message interactions
            for sender_id, receiver_id, message_count, last_interaction in message_patterns:
                weight = min(message_count * 0.15, 1.0)
                session.run("""
                    MATCH (e1:Expert {id: $sender_id})
                    MATCH (e2:Expert {id: $receiver_id})
                    MERGE (e1)-[r:INTERACTS_WITH]->(e2)
                    SET r.weight = $weight,
                        r.message_count = $message_count,
                        r.last_updated = datetime()
                """, {
                    "sender_id": str(sender_id),
                    "receiver_id": str(receiver_id),
                    "weight": weight,
                    "message_count": message_count
                })
                
            logger.info("Created adaptive relationships based on historical patterns")
        except Exception as e:
            logger.error(f"Error creating adaptive relationships: {e}")

    def _process_expertise(self, expertise_list: List[str]) -> Dict[str, List[str]]:
        """Process expertise list and return standardized format"""
        try:
            if not expertise_list:
                return {
                    'concepts': [],
                    'areas': [],
                    'methods': [],
                    'related': []
                }

            prompt = f"""Return only a raw JSON object with these keys for this expertise list: {expertise_list}
            {{
                "standardized_concepts": [],
                "research_areas": [],
                "methods": [],
                "related_areas": []
            }}
            Return the JSON object only, no markdown formatting, no code fences, no additional text."""
            
            try:
                response = model.generate_content(prompt)
                    
                if not response.text or not response.text.strip():
                    logger.error("Received empty response from Gemini")
                    return {
                        'concepts': expertise_list,
                        'areas': [],
                        'methods': [],
                        'related': []
                    }

                cleaned_response = (response.text
                                .replace('```json', '')
                                .replace('```JSON', '')
                                .replace('```', '')
                                .strip())
                
                parsed = json.loads(cleaned_response)
                return {
                    'concepts': parsed.get('standardized_concepts', expertise_list),
                    'areas': parsed.get('research_areas', []),
                    'methods': parsed.get('methods', []),
                    'related': parsed.get('related_areas', [])
                }
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON response from Gemini: {response.text}")
                return {
                    'concepts': expertise_list,
                    'areas': [],
                    'methods': [],
                    'related': []
                }
            except Exception as e:
                if '429' in str(e):
                    logger.error("Rate limit exceeded, using default values")
                else:
                    logger.error(f"Error in Gemini API call: {e}")
                return {
                    'concepts': expertise_list,
                    'areas': [],
                    'methods': [],
                    'related': []
                }

        except Exception as e:
            logger.error(f"Error in expertise processing: {e}")
            return {
                'concepts': expertise_list,
                'areas': [],
                'methods': [],
                'related': []
            }

    def create_expert_node(self, session, expert_data: tuple):
        """Create expert node with semantic relationships"""
        try:
            # Unpack expert data
            (expert_id, first_name, last_name, knowledge_expertise, designation, 
             domains, fields, theme, unit, orcid, is_active) = expert_data
            
            expert_name = f"{first_name} {last_name}"

            # Create basic expert node
            session.run("""
                MERGE (e:Expert {id: $id})
                SET e.name = $name,
                    e.designation = $designation,
                    e.theme = $theme,
                    e.unit = $unit,
                    e.orcid = $orcid,
                    e.is_active = $is_active,
                    e.updated_at = datetime()
            """, {
                "id": str(expert_id),
                "name": expert_name,
                "designation": designation,
                "theme": theme,
                "unit": unit,
                "orcid": orcid,
                "is_active": is_active
            })

            # Process expertise semantically
            semantic_data = self._process_expertise(knowledge_expertise)

            # Create semantic relationships
            self._create_semantic_relationships(session, str(expert_id), semantic_data)

            # Create field relationships
            if fields:
                for field in fields:
                    session.run("""
                        MERGE (f:Field {name: $field})
                        MERGE (e:Expert {id: $expert_id})-[r:SPECIALIZES_IN]->(f)
                        SET r.weight = 0.9,
                            r.last_updated = datetime()
                    """, {
                        "expert_id": str(expert_id),
                        "field": field
                    })

            # Create domain relationships
            if domains:
                for domain in domains:
                    session.run("""
                        MERGE (d:Domain {name: $domain})
                        MERGE (e:Expert {id: $expert_id})-[r:WORKS_IN_DOMAIN]->(d)
                        SET r.weight = 0.85,
                            r.last_updated = datetime()
                    """, {
                        "expert_id": str(expert_id),
                        "domain": domain
                    })

            # Create organizational relationships
            if theme:
                session.run("""
                    MERGE (t:Theme {name: $theme})
                    MERGE (e:Expert {id: $expert_id})-[r:BELONGS_TO_THEME]->(t)
                    SET r.last_updated = datetime()
                """, {
                    "expert_id": str(expert_id),
                    "theme": theme
                })

            if unit:
                session.run("""
                    MERGE (u:Unit {name: $unit})
                    MERGE (e:Expert {id: $expert_id})-[r:BELONGS_TO_UNIT]->(u)
                    SET r.last_updated = datetime()
                """, {
                    "expert_id": str(expert_id),
                    "unit": unit
                })

            logger.info(f"Successfully created/updated expert node: {expert_name}")

        except Exception as e:
            logger.error(f"Error creating expert node for {expert_id}: {e}")
            raise

    def _create_semantic_relationships(self, session, expert_id: str, semantic_data: Dict[str, List[str]]):
        """Create semantic relationships for an expert"""
        try:
            # Create concept relationships
            for concept in semantic_data['concepts']:
                session.run("""
                    MERGE (c:Concept {name: $concept})
                    MERGE (e:Expert {id: $expert_id})-[r:HAS_CONCEPT]->(c)
                    SET r.weight = 1.0,
                        r.last_updated = datetime()
                """, {
                    "expert_id": expert_id,
                    "concept": concept
                })

            # Create research area relationships
            for area in semantic_data['areas']:
                session.run("""
                    MERGE (ra:ResearchArea {name: $area})
                    MERGE (e:Expert {id: $expert_id})-[r:RESEARCHES_IN]->(ra)
                    SET r.weight = 0.8,
                        r.last_updated = datetime()
                """, {
                    "expert_id": expert_id,
                    "area": area
                })

            # Create method relationships
            for method in semantic_data['methods']:
                session.run("""
                    MERGE (m:Method {name: $method})
                    MERGE (e:Expert {id: $expert_id})-[r:USES_METHOD]->(m)
                    SET r.weight = 0.7,
                        r.last_updated = datetime()
                """, {
                    "expert_id": expert_id,
                    "method": method
                })

            # Create related area relationships
            for related in semantic_data['related']:
                session.run("""
                    MERGE (r:RelatedArea {name: $related})
                    MERGE (e:Expert {id: $expert_id})-[r:RELATED_TO]->(r)
                    SET r.weight = 0.5,
                        r.last_updated = datetime()
                """, {
                    "expert_id": expert_id,
                    "related": related
                })

        except Exception as e:
            logger.error(f"Error creating semantic relationships for expert {expert_id}: {e}")
            raise

    async def initialize_graph(self):
        """Initialize the graph with experts and their relationships"""
        try:
            # Create indexes first
            self._create_indexes()
            
            # Fetch experts data
            experts_data = self._fetch_experts_data()
            
            if not experts_data:
                logger.warning("No experts data found to process")
                return False

            # Process each expert
            with self._neo4j_driver.session() as session:
                for expert_data in experts_data:
                    try:
                        self.create_expert_node(session, expert_data)
                    except Exception as e:
                        logger.error(f"Error processing expert data: {e}")
                        continue

                # Add adaptive relationships based on historical data
                search_patterns, message_patterns = self._process_historical_data()
                self._create_adaptive_relationships(session, search_patterns, message_patterns)

            logger.info("Graph initialization complete with adaptive relationships!")
            return True

        except Exception as e:
            logger.error(f"Graph initialization failed: {e}")
            return False

    def close(self):
        """Close the Neo4j driver"""
        if self._neo4j_driver:
            self._neo4j_driver.close()
            logger.info("Neo4j driver closed")