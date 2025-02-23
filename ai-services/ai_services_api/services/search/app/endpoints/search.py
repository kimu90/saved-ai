from fastapi import APIRouter, HTTPException, Request, Depends
from typing import List, Dict, Optional
from pydantic import BaseModel
import logging
from datetime import datetime, timezone
import json
import pandas as pd
from redis.asyncio import Redis
import uuid

from ai_services_api.services.search.indexing.index_creator import ExpertSearchIndexManager
from ai_services_api.services.search.ml.ml_predictor import MLPredictor
from ai_services_api.services.message.core.database import get_db_connection

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

router = APIRouter()

# Constants
TEST_USER_ID = "123"

# Initialize ML Predictor
ml_predictor = MLPredictor()
logger.info("ML Predictor initialized successfully")

# Response Models [unchanged]
class ExpertSearchResult(BaseModel):
    id: str
    first_name: str
    last_name: str
    designation: str
    theme: str
    unit: str
    contact: str
    is_active: bool
    score: float = None
    bio: str = None  
    knowledge_expertise: List[str] = []

class SearchResponse(BaseModel):
    total_results: int
    experts: List[ExpertSearchResult]
    user_id: str
    session_id: str

class PredictionResponse(BaseModel):
    predictions: List[str]
    confidence_scores: List[float]
    user_id: str

async def get_redis():
    logger.debug("Initializing Redis connection")
    redis_client = Redis(host='redis', port=6379, db=3, decode_responses=True)
    logger.info("Redis connection established")
    return redis_client

async def get_user_id(request: Request) -> str:
    logger.debug("Extracting user ID from request headers")
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        logger.error("Missing required X-User-ID header in request")
        raise HTTPException(status_code=400, detail="X-User-ID header is required")
    logger.info(f"User ID extracted successfully: {user_id}")
    return user_id

async def get_test_user_id(request: Request) -> str:
    logger.debug("Using test user ID")
    print(f"Test user ID being used: {TEST_USER_ID}")
    return TEST_USER_ID

async def get_or_create_session(conn, user_id: str) -> str:
    logger.info(f"Getting or creating session for user: {user_id}")
    cur = conn.cursor()
    try:
        session_id = int(str(int(datetime.utcnow().timestamp()))[-8:])
        print(f"Generated session ID: {session_id}")
        
        cur.execute("""
            INSERT INTO search_sessions 
                (session_id, user_id, start_timestamp, is_active)
            VALUES (%s, %s, CURRENT_TIMESTAMP, true)
            RETURNING session_id
        """, (session_id, user_id))
        
        conn.commit()
        logger.debug(f"Session created successfully with ID: {session_id}")
        return str(session_id)
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating session: {str(e)}", exc_info=True)
        print(f"Session creation failed: {str(e)}")
        raise
    finally:
        cur.close()

async def record_search(conn, session_id: str, user_id: str, query: str, results: List[Dict], response_time: float):
    logger.info(f"Recording search analytics - Session: {session_id}, User: {user_id}")
    print(f"Recording search for query: {query} with {len(results)} results")
    
    cur = conn.cursor()
    try:
        # Record search analytics
        cur.execute("""
            INSERT INTO search_analytics
                (search_id, query, user_id, response_time,
                 result_count, search_type, timestamp)
            VALUES
                ((SELECT id FROM search_sessions WHERE session_id = %s),
                %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            session_id,
            query,
            user_id,
            response_time,
            len(results),
            'expert_search'
        ))
        
        search_id = cur.fetchone()[0]
        logger.debug(f"Created search analytics record with ID: {search_id}")

        # Record top 5 expert matches
        print(f"Recording top 5 matches from {len(results)} total results")
        for rank, result in enumerate(results[:5], 1):
            cur.execute("""
                INSERT INTO expert_search_matches
                    (search_id, expert_id, rank_position, similarity_score)
                VALUES (%s, %s, %s, %s)
            """, (
                search_id,
                result["id"],
                rank,
                result.get("score", 0.0)
            ))
            logger.debug(f"Recorded match - Expert ID: {result['id']}, Rank: {rank}")

        conn.commit()
        logger.info(f"Successfully recorded all search data for search ID: {search_id}")
        return search_id
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error recording search: {str(e)}", exc_info=True)
        print(f"Search recording failed: {str(e)}")
        raise
    finally:
        cur.close()

async def record_prediction(conn, session_id: str, user_id: str, partial_query: str, predictions: List[str], confidence_scores: List[float]):
    logger.info(f"Recording predictions for user {user_id}, session {session_id}")
    print(f"Recording predictions for partial query: {partial_query}")
    
    cur = conn.cursor()
    try:
        for pred, conf in zip(predictions, confidence_scores):
            cur.execute("""
                INSERT INTO query_predictions
                    (partial_query, predicted_query, confidence_score, 
                    user_id, timestamp)
                VALUES 
                    (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (partial_query, pred, conf, user_id))
            logger.debug(f"Recorded prediction: {pred} with confidence: {conf}")
        
        conn.commit()
        logger.info("Successfully recorded all predictions")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error recording prediction: {str(e)}", exc_info=True)
        print(f"Prediction recording failed: {str(e)}")
        raise
    finally:
        cur.close()

async def process_expert_search(query: str, user_id: str, active_only: bool = True, redis_client: Redis = None) -> SearchResponse:
    logger.info(f"Processing expert search - Query: {query}, User: {user_id}")
    print(f"Starting expert search process for query: {query}")
    
    conn = None
    try:
        if redis_client:
            cache_key = f"expert_search:{user_id}:{query}:{active_only}"
            cached_response = await redis_client.get(cache_key)
            if cached_response:
                logger.info(f"Cache hit for search: {cache_key}")
                print("Retrieved results from cache")
                return SearchResponse(**json.loads(cached_response))

        conn = get_db_connection()
        session_id = await get_or_create_session(conn, user_id)
        logger.debug(f"Created session: {session_id}")
        
        # Execute search
        start_time = datetime.utcnow()
        search_manager = ExpertSearchIndexManager()
        results = search_manager.search_experts(query, k=5, active_only=active_only)
        response_time = (datetime.utcnow() - start_time).total_seconds()
        
        logger.info(f"Search completed in {response_time:.2f} seconds with {len(results)} results")
        print(f"Found {len(results)} experts in {response_time:.2f} seconds")

        formatted_results = [
            ExpertSearchResult(
                id=str(result['id']),
                first_name=result['first_name'],
                last_name=result['last_name'],
                designation=result.get('designation', ''),
                theme=result.get('theme', ''),
                unit=result.get('unit', ''),
                contact=result.get('contact', ''),
                is_active=result.get('is_active', True),
                score=result.get('score'),
                bio=result.get('bio'),
                knowledge_expertise=result.get('knowledge_expertise', [])
            ) for result in results
        ]
        
        await record_search(conn, session_id, user_id, query, results, response_time)
        
        try:
            ml_predictor.update(query, user_id=user_id)
            logger.debug("ML predictor updated successfully")
        except Exception as e:
            logger.error(f"ML predictor update failed: {str(e)}", exc_info=True)
            print(f"ML predictor update error: {str(e)}")
        
        response = SearchResponse(
            total_results=len(formatted_results),
            experts=formatted_results,
            user_id=user_id,
            session_id=session_id
        )

        if redis_client:
            await redis_client.setex(
                cache_key,
                3600,
                json.dumps(response.dict())
            )
            logger.debug(f"Cached search results with key: {cache_key}")
        
        return response
        
    except Exception as e:
        logger.error(f"Error searching experts: {str(e)}", exc_info=True)
        print(f"Expert search failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Search processing failed")
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

async def process_query_prediction(partial_query: str, user_id: str, redis_client: Redis = None) -> PredictionResponse:
    logger.info(f"Processing query prediction - Partial query: {partial_query}, User: {user_id}")
    print(f"Starting prediction process for partial query: {partial_query}")
    
    conn = None
    try:
        if redis_client:
            cache_key = f"query_prediction:{user_id}:{partial_query}"
            cached_response = await redis_client.get(cache_key)
            if cached_response:
                logger.info(f"Cache hit for prediction: {cache_key}")
                print("Retrieved predictions from cache")
                return PredictionResponse(**json.loads(cached_response))

        conn = get_db_connection()
        session_id = await get_or_create_session(conn, user_id)
        
        predictions = ml_predictor.predict(partial_query, user_id=user_id)
        confidence_scores = [1.0 - (i * 0.1) for i in range(len(predictions))]
        
        logger.debug(f"Generated {len(predictions)} predictions")
        print(f"Generated predictions: {predictions}")
        
        await record_prediction(
            conn,
            session_id,
            user_id,
            partial_query,
            predictions,
            confidence_scores
        )
        
        response = PredictionResponse(
            predictions=predictions,
            confidence_scores=confidence_scores,
            user_id=user_id
        )

        if redis_client:
            await redis_client.setex(
                cache_key,
                1800,
                json.dumps(response.dict())
            )
            logger.debug(f"Cached predictions with key: {cache_key}")
        
        return response
        
    except Exception as e:
        logger.error(f"Error predicting queries: {str(e)}", exc_info=True)
        print(f"Query prediction failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Prediction failed")
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

# API Endpoints
@router.get("/experts/search/{query}")
async def search_experts(
    query: str,
    request: Request,
    active_only: bool = True,
    user_id: str = Depends(get_user_id),
    redis_client: Redis = Depends(get_redis)
):
    logger.info(f"Received expert search request - Query: {query}, User: {user_id}")
    return await process_expert_search(query, user_id, active_only, redis_client)

@router.get("/experts/predict/{partial_query}")
async def predict_query(
    partial_query: str,
    request: Request,
    user_id: str = Depends(get_user_id),
    redis_client: Redis = Depends(get_redis)
):
    logger.info(f"Received query prediction request - Partial query: {partial_query}, User: {user_id}")
    return await process_query_prediction(partial_query, user_id, redis_client)

@router.get("/test/experts/search/{query}")
async def test_search_experts(
    query: str,
    request: Request,
    active_only: bool = True,
    user_id: str = Depends(get_test_user_id),
    redis_client: Redis = Depends(get_redis)
):
    logger.info(f"Received test expert search request - Query: {query}")
    return await process_expert_search(query, user_id, active_only, redis_client)

@router.get("/test/experts/predict/{partial_query}")
async def test_predict_query(
    partial_query: str,
    request: Request,
    user_id: str = Depends(get_test_user_id),
    redis_client: Redis = Depends(get_redis)
):
    logger.info(f"Received test query prediction request - Partial query: {partial_query}")
    return await process_query_prediction(partial_query, user_id, redis_client)