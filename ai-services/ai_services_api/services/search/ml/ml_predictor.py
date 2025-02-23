import logging
from typing import List, Dict, Optional, Any
from collections import defaultdict
from datetime import datetime, timedelta
from contextlib import contextmanager
from ai_services_api.services.centralized_repository.database_setup import get_db_connection

logger = logging.getLogger(__name__)

class MLPredictor:
    def __init__(self):
        self.prefix_tree = defaultdict(dict)  # User-specific prefix trees
        self.query_freq = defaultdict(lambda: defaultdict(int))
        self.recent_queries = defaultdict(list)
        self.max_recent = 1000
        self.time_window = 24
        self._initialize_db_connection()

    def _initialize_db_connection(self):
        """Initialize database connection and cursor"""
        try:
            with get_db_connection() as connection:
                self.conn = connection
                self.cur = self.conn.cursor()
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise

    def _ensure_connection(self):
        """Ensure database connection is active, reconnect if necessary"""
        try:
            # Try a simple query to test connection
            self.cur.execute("SELECT 1")
        except Exception as e:
            logger.warning(f"Database connection lost, attempting to reconnect: {e}")
            self._initialize_db_connection()

    def _execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a database query and return results as dictionaries"""
        try:
            self._ensure_connection()
            self.cur.execute(query, params)
            if self.cur.description:
                columns = [desc[0] for desc in self.cur.description]
                results = self.cur.fetchall()
                return [dict(zip(columns, row)) for row in results]
            return []
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Database query failed: {str(e)}\nQuery: {query}\nParams: {params}")
            raise

    def _add_to_prefix_tree(self, query: str, user_id: str):
        """Add a query to the user's prefix tree"""
        current = self.prefix_tree[user_id]
        query = query.lower()
        for char in query:
            if char not in current:
                current[char] = {}
            current = current[char]
        if '_end_' not in current:
            current['_end_'] = set()
        current['_end_'].add(query)

    def _get_from_prefix_tree(self, prefix: str, user_id: str, limit: int) -> List[str]:
        """Get all queries starting with prefix from user's tree"""
        if user_id not in self.prefix_tree:
            return []
            
        current = self.prefix_tree[user_id]
        prefix = prefix.lower()
        
        # Navigate to prefix node
        for char in prefix:
            if char not in current:
                return []
            current = current[char]
            
        # Collect all complete words from this point
        results = []
        def collect_words(node, limit):
            if '_end_' in node:
                results.extend(node['_end_'])
            if len(results) >= limit:
                return
            for char in node:
                if char != '_end_':
                    collect_words(node[char], limit)
                    
        collect_words(current, limit)
        return results[:limit]

    def _get_user_training_data(self, user_id: str) -> List[Dict]:
        """Get comprehensive user search history with click and success metrics"""
        try:
            query = """
            WITH UserSearches AS (
                SELECT 
                    sl.query,
                    COUNT(*) as search_count,
                    MAX(sl.timestamp) as last_used,
                    SUM(CASE WHEN sl.clicked THEN 1 ELSE 0 END)::float / COUNT(*) as click_rate,
                    AVG(sl.success_rate) as avg_success_rate,
                    array_agg(DISTINCT es.expert_id) as clicked_experts
                FROM search_logs sl
                LEFT JOIN expert_searches es 
                    ON sl.id = es.search_id AND es.clicked = true
                WHERE sl.user_id = %s
                AND sl.timestamp >= NOW() - INTERVAL '30 days'
                GROUP BY sl.query
            )
            SELECT 
                query,
                search_count,
                last_used,
                click_rate,
                avg_success_rate,
                clicked_experts
            FROM UserSearches
            ORDER BY last_used DESC
            """
            return self._execute_query(query, (user_id,))
        except Exception as e:
            logger.error(f"Error getting user training data: {e}")
            return []

    def train_user_model(self, user_id: str):
        """Train predictor with user-specific search patterns"""
        try:
            # Get comprehensive user history
            user_history = self._get_user_training_data(user_id)
            
            # Reset user's data structures
            self.prefix_tree[user_id] = {}
            self.query_freq[user_id].clear()
            self.recent_queries[user_id] = []
            
            for record in user_history:
                query = record['query']
                search_count = record['search_count']
                last_used = record['last_used']
                click_rate = record['click_rate']
                success_rate = record['avg_success_rate']
                
                # Calculate query weight based on multiple factors
                weight = search_count * (1 + click_rate) * (1 + success_rate)
                
                # Add to prefix tree
                self._add_to_prefix_tree(query, user_id)
                self.query_freq[user_id][query.lower()] = weight
                
                # Add to recent queries if within time window
                if datetime.now() - last_used < timedelta(hours=self.time_window):
                    self.recent_queries[user_id].append({
                        'query': query,
                        'timestamp': last_used,
                        'weight': weight
                    })
            
            logger.info(f"Trained model for user {user_id} with {len(user_history)} queries")
            
        except Exception as e:
            logger.error(f"Error training user model: {e}")

    def predict(self, partial_query: str, user_id: str, limit: int = 5) -> List[str]:
        """Predict queries with personalized ranking and improved error handling"""
        try:
            if not partial_query or len(partial_query) < 2:
                logger.debug(f"Skipping prediction: partial query too short or empty: {partial_query}")
                return []
                
            # Ensure user model is trained
            if user_id not in self.prefix_tree:
                logger.debug(f"Training model for new user: {user_id}")
                self.train_user_model(user_id)
            
            # Get matching queries
            matches = self._get_from_prefix_tree(partial_query, user_id, limit * 2)
            scored_matches = []
            current_time = datetime.now()
            
            logger.debug(f"Found {len(matches)} initial matches for partial query: {partial_query}")
            
            for query in matches:
                base_score = 0
                
                # Get detailed query metrics with better error handling
                metrics_query = """
                    SELECT 
                        COALESCE(COUNT(*), 0) as usage_count,
                        COALESCE(SUM(CASE WHEN clicked THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0), 0) as click_rate,
                        COALESCE(AVG(success_rate), 0) as success_rate,
                        MAX(timestamp) as last_used,
                        COALESCE(COUNT(DISTINCT user_id), 0) as user_count
                    FROM search_logs
                    WHERE query = %s AND user_id = %s
                    AND timestamp >= NOW() - INTERVAL '30 days'
                    GROUP BY query
                """
                
                try:
                    metrics = self._execute_query(metrics_query, (query, user_id))
                    
                    if metrics and metrics[0]:
                        m = metrics[0]
                        
                        # Safe extraction of metrics with defaults
                        usage_count = float(m.get('usage_count', 0) or 0)
                        click_rate = float(m.get('click_rate', 0) or 0)
                        success_rate = float(m.get('success_rate', 0) or 0)
                        last_used = m.get('last_used')
                        
                        # Usage frequency score (0-3)
                        base_score += min(usage_count / 5, 3)
                        
                        # Click-through rate score (0-2)
                        base_score += click_rate * 2
                        
                        # Success rate score (0-2)
                        base_score += success_rate * 2
                        
                        # Recency score (0-2)
                        if last_used:
                            days_old = (current_time - last_used).days
                            base_score += max(0, 2 - (days_old * 0.1))
                        
                        # Add base frequency from prefix tree (with safety check)
                        freq_score = self.query_freq[user_id].get(query.lower(), 0)
                        base_score += float(freq_score) * 0.1
                    
                    logger.debug(f"Query '{query}' scored {base_score}")
                    scored_matches.append((query, base_score))
                    
                except Exception as metric_error:
                    logger.error(f"Error calculating metrics for query '{query}': {metric_error}")
                    # Add with minimal score to keep in results but ranked lower
                    scored_matches.append((query, 0.1))
            
            # Sort by score and return top matches
            scored_matches.sort(key=lambda x: x[1], reverse=True)
            predictions = [query for query, score in scored_matches[:limit]]
            
            try:
                # Record predictions in database with error handling
                for pred in predictions:
                    self._execute_query("""
                        INSERT INTO query_predictions 
                        (partial_query, predicted_query, confidence_score, user_id)
                        VALUES (%s, %s, %s, %s)
                    """, (partial_query, pred, scored_matches[predictions.index(pred)][1], user_id))
                
                self.conn.commit()
                
            except Exception as db_error:
                logger.error(f"Error recording predictions: {db_error}")
                # Continue even if recording fails
            
            logger.debug(f"Returning {len(predictions)} predictions for '{partial_query}'")
            return predictions
                
        except Exception as e:
            logger.error(f"Error in personalized prediction: {str(e)}")
            return []

    def update(self, new_query: str, user_id: str = None):
        """Update the model with a new query"""
        try:
            new_query = new_query.strip()
            if not new_query or not user_id:
                return
                
            # Add to user's prefix tree
            self._add_to_prefix_tree(new_query, user_id)
            
            # Update frequency
            self.query_freq[user_id][new_query.lower()] += 1
            
            # Update recent queries
            self.recent_queries[user_id].append({
                'query': new_query,
                'timestamp': datetime.now(),
                'weight': 1  # Base weight for new queries
            })
            
            # Maintain recent queries limit
            if len(self.recent_queries[user_id]) > self.max_recent:
                self.recent_queries[user_id] = self.recent_queries[user_id][-self.max_recent:]
                
        except Exception as e:
            logger.error(f"Error updating ML predictor: {e}")

    def train(self, historical_queries: List[str], user_id: str = "default"):
        """Train the predictor on historical queries"""
        try:
            if not historical_queries:
                return
                
            # Reset user's data structures
            self.prefix_tree[user_id] = {}
            self.query_freq[user_id].clear()
            
            # Process each query
            for query in historical_queries:
                query = query.strip()
                if not query:
                    continue
                    
                self._add_to_prefix_tree(query, user_id)
                self.query_freq[user_id][query.lower()] += 1
                
            logger.info(f"Trained user {user_id} model on {len(historical_queries)} queries")
            
        except Exception as e:
            logger.error(f"Error training ML predictor: {e}")

    def close(self):
        """Close database connection"""
        if hasattr(self, 'cur') and self.cur:
            self.cur.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def __del__(self):
        """Destructor to ensure connection is closed"""
        self.close()
