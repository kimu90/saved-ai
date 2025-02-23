import logging
from typing import List, Dict, Optional, Any
from collections import defaultdict
from datetime import datetime, timedelta
from contextlib import contextmanager
from ai_services_api.services.centralized_repository.database_setup import get_db_connection
from redis import Redis
import json

logger = logging.getLogger(__name__)

class MLPredictor:
    def __init__(self):
        # Keep original structures as fallback
        self.prefix_tree = defaultdict(dict)
        self.query_freq = defaultdict(lambda: defaultdict(int))
        self.recent_queries = defaultdict(list)
        self.max_recent = 1000
        self.time_window = 24
        
        # Add Redis client for speed
        self.redis_client = Redis(
            host='redis',
            port=6379,
            db=5,
            decode_responses=True
        )
        
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

    def predict(self, partial_query: str, user_id: str, limit: int = 5) -> List[str]:
        """Predict queries with Redis-accelerated lookup"""
        try:
            if not partial_query or len(partial_query) < 2:
                return []

            # Try Redis cache first
            cache_key = f"predict:{user_id}:{partial_query}"
            cached = self.redis_client.get(cache_key)
            if cached:
                return json.loads(cached)

            # Get predictions from Redis sorted set
            prefix_key = f"prefix:{user_id}:{partial_query.lower()}"
            matches = self.redis_client.zrevrange(prefix_key, 0, limit-1)

            if not matches:
                # Fallback to original prediction method
                matches = self._get_from_prefix_tree(partial_query, user_id, limit * 2)
                
                # Store in Redis for next time
                if matches:
                    pipeline = self.redis_client.pipeline()
                    for idx, match in enumerate(matches):
                        score = self.query_freq[user_id].get(match.lower(), 0)
                        pipeline.zadd(prefix_key, {match: score})
                    pipeline.expire(prefix_key, 3600)  # Expire in 1 hour
                    pipeline.execute()

            # Score and sort matches
            scored_matches = []
            for query in matches:
                # Get score from Redis
                score_key = f"score:{user_id}:{query.lower()}"
                score = self.redis_client.get(score_key)
                
                if score is None:
                    # Calculate score using original metrics
                    metrics_query = """
                        SELECT 
                            COALESCE(COUNT(*), 0) as usage_count,
                            COALESCE(SUM(CASE WHEN clicked THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0), 0) as click_rate,
                            COALESCE(AVG(success_rate), 0) as success_rate,
                            MAX(timestamp) as last_used
                        FROM search_logs
                        WHERE query = %s AND user_id = %s
                        AND timestamp >= NOW() - INTERVAL '30 days'
                        GROUP BY query
                    """
                    metrics = self._execute_query(metrics_query, (query, user_id))
                    
                    if metrics and metrics[0]:
                        m = metrics[0]
                        score = (
                            float(m.get('usage_count', 0) or 0) * 0.3 +
                            float(m.get('click_rate', 0) or 0) * 0.4 +
                            float(m.get('success_rate', 0) or 0) * 0.3
                        )
                        # Cache score in Redis
                        self.redis_client.setex(score_key, 3600, str(score))
                    else:
                        score = 0.1

                scored_matches.append((query, float(score)))

            # Sort and get top matches
            scored_matches.sort(key=lambda x: x[1], reverse=True)
            predictions = [query for query, _ in scored_matches[:limit]]

            # Cache final predictions
            self.redis_client.setex(cache_key, 300, json.dumps(predictions))  # Cache for 5 minutes

            return predictions

        except Exception as e:
            logger.error(f"Error in prediction: {str(e)}")
            # Fallback to original method if Redis fails
            return self._get_from_prefix_tree(partial_query, user_id, limit)

    def update(self, new_query: str, user_id: str = None):
        """Update the model with a new query"""
        try:
            if not new_query or not user_id:
                return

            new_query = new_query.strip().lower()
            
            # Update Redis
            pipeline = self.redis_client.pipeline()
            
            # Update query frequency
            freq_key = f"freq:{user_id}:{new_query}"
            pipeline.incr(freq_key)
            
            # Update recent queries
            recent_key = f"recent:{user_id}"
            pipeline.zadd(recent_key, {new_query: datetime.now().timestamp()})
            pipeline.zremrangebyrank(recent_key, 0, -self.max_recent-1)
            
            # Store all prefixes for faster lookups
            for i in range(1, len(new_query) + 1):
                prefix = new_query[:i]
                prefix_key = f"prefix:{user_id}:{prefix}"
                pipeline.zadd(prefix_key, {new_query: datetime.now().timestamp()})
                pipeline.expire(prefix_key, 86400)  # Expire in 24 hours
            
            pipeline.execute()

            # Update original structures as backup
            self._add_to_prefix_tree(new_query, user_id)
            self.query_freq[user_id][new_query] += 1
            self.recent_queries[user_id].append({
                'query': new_query,
                'timestamp': datetime.now(),
                'weight': 1
            })

            if len(self.recent_queries[user_id]) > self.max_recent:
                self.recent_queries[user_id] = self.recent_queries[user_id][-self.max_recent:]

        except Exception as e:
            logger.error(f"Error updating predictor: {e}")
            # Continue with original update if Redis fails
            super().update(new_query, user_id)

    def train(self, historical_queries: List[str], user_id: str = "default"):
        """Train the predictor on historical queries"""
        try:
            if not historical_queries:
                return

            pipeline = self.redis_client.pipeline()
            
            # Clear existing data for user
            user_keys = self.redis_client.keys(f"*:{user_id}:*")
            if user_keys:
                pipeline.delete(*user_keys)

            # Process each query
            for query in historical_queries:
                query = query.strip().lower()
                if not query:
                    continue

                # Store in Redis
                for i in range(1, len(query) + 1):
                    prefix = query[:i]
                    prefix_key = f"prefix:{user_id}:{prefix}"
                    pipeline.zadd(prefix_key, {query: 1.0})
                
                freq_key = f"freq:{user_id}:{query}"
                pipeline.incr(freq_key)

            pipeline.execute()

            # Update original structures as backup
            self.prefix_tree[user_id] = {}
            self.query_freq[user_id].clear()
            for query in historical_queries:
                self._add_to_prefix_tree(query, user_id)
                self.query_freq[user_id][query.lower()] += 1

        except Exception as e:
            logger.error(f"Error training predictor: {e}")
            # Fallback to original training if Redis fails
            super().train(historical_queries, user_id)

    def train_user_model(self, user_id: str):
        """Train predictor with user-specific search patterns"""
        try:
            # Get comprehensive user history
            user_history = self._get_user_training_data(user_id)
            
            # Reset user's data structures
            self.prefix_tree[user_id] = {}
            self.query_freq[user_id].clear()
            self.recent_queries[user_id] = []
            
            pipeline = self.redis_client.pipeline()
            
            # Clear existing Redis data for user
            user_keys = self.redis_client.keys(f"*:{user_id}:*")
            if user_keys:
                pipeline.delete(*user_keys)
            
            for record in user_history:
                query = record['query']
                search_count = record['search_count']
                last_used = record['last_used']
                click_rate = record['click_rate']
                success_rate = record['avg_success_rate']
                
                # Calculate query weight based on multiple factors
                weight = search_count * (1 + click_rate) * (1 + success_rate)
                
                # Add to Redis
                query_lower = query.lower()
                
                # Store prefix matches
                for i in range(1, len(query_lower) + 1):
                    prefix = query_lower[:i]
                    prefix_key = f"prefix:{user_id}:{prefix}"
                    pipeline.zadd(prefix_key, {query: weight})
                    pipeline.expire(prefix_key, 86400)  # 24 hour expiry
                
                # Store query frequency
                freq_key = f"freq:{user_id}:{query_lower}"
                pipeline.set(freq_key, search_count)
                
                # Store score
                score_key = f"score:{user_id}:{query_lower}"
                pipeline.set(score_key, weight)
                
                # Add to original structures as backup
                self._add_to_prefix_tree(query, user_id)
                self.query_freq[user_id][query_lower] = weight
                
                # Add to recent queries if within time window
                if datetime.now() - last_used < timedelta(hours=self.time_window):
                    self.recent_queries[user_id].append({
                        'query': query,
                        'timestamp': last_used,
                        'weight': weight
                    })
            
            pipeline.execute()
            logger.info(f"Trained model for user {user_id} with {len(user_history)} queries")
            
        except Exception as e:
            logger.error(f"Error training user model: {e}")

    def close(self):
        """Close all connections"""
        try:
            self.redis_client.close()
        except:
            pass
        if hasattr(self, 'cur') and self.cur:
            self.cur.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def __del__(self):
        """Ensure connections are closed"""
        self.close()