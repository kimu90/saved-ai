from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import asyncio
import logging
import sys
import os

# Import the SystemInitializer
from ai_services_api.services.centralized_repository.system_initializer import (
    SystemInitializer, 
    SetupConfig
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def execute_system_initialization(**context):
    """
    Wrapper function to execute system initialization in Airflow context
    """
    logger.info("Starting comprehensive research data processing")
    
    try:
        # Dynamically create SetupConfig from Airflow Variables
        setup_config = SetupConfig(
            skip_database=Variable.get("skip_database", default_var="false").lower() == "true",
            skip_openalex=Variable.get("skip_openalex", default_var="false").lower() == "true",
            skip_publications=Variable.get("skip_publications", default_var="false").lower() == "true",
            skip_graph=Variable.get("skip_graph", default_var="false").lower() == "true",
            skip_search=Variable.get("skip_search", default_var="false").lower() == "true",
            skip_redis=Variable.get("skip_redis", default_var="false").lower() == "true",
            skip_scraping=Variable.get("skip_scraping", default_var="false").lower() == "true",
            expertise_csv=Variable.get("expertise_csv", default_var="experts.csv"),
            max_pages=int(Variable.get("max_pages", default_var=1000)),
            max_workers=int(Variable.get("max_workers", default_var=4))
        )
        
        # Create SystemInitializer with the config
        initializer = SystemInitializer(setup_config)
        
        # Run the async initialization
        if os.name == 'nt':  # Windows specific event loop policy
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        # Use asyncio.run to execute the async method
        asyncio.run(initializer.initialize_system())
        
        logger.info("Comprehensive research data processing completed successfully")
        return "System initialization completed successfully"
    
    except Exception as e:
        logger.error(f"System initialization failed: {str(e)}", exc_info=True)
        raise

def verify_dependencies(**context):
    """
    Pre-flight checks for system dependencies
    """
    try:
        # Verify required environment variables
        required_vars = [
            'DATABASE_URL', 'NEO4J_URI', 'NEO4J_USER', 'NEO4J_PASSWORD', 
            'OPENALEX_API_URL', 'GEMINI_API_KEY', 'REDIS_URL',
            'ORCID_CLIENT_ID', 'ORCID_CLIENT_SECRET', 'KNOWHUB_BASE_URL'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")
        
        logger.info("All system dependencies verified successfully")
        return True
    except Exception as e:
        logger.error(f"Dependency verification failed: {str(e)}")
        raise

def post_processing_cleanup(**context):
    """
    Perform post-processing cleanup and notifications
    """
    try:
        logger.info("Executing post-processing cleanup")
        # Add any additional cleanup, logging, or notification tasks
        # For example:
        # - Generate summary reports
        # - Send email notifications
        # - Perform data integrity checks
        logger.info("Post-processing cleanup completed")
        return True
    except Exception as e:
        logger.error(f"Post-processing cleanup failed: {str(e)}")
        raise

# Create the DAG
with DAG(
    'comprehensive_research_processing',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=60)
    },
    description='Comprehensive research data processing pipeline',
    schedule_interval='0 0 1 * *',  # Run monthly on the first day
    start_date=days_ago(2),
    catchup=False,
    tags=['research', 'monthly', 'data-processing'],
    max_active_runs=1,
    sla=timedelta(hours=12)
) as dag:
    
    # Dependency verification task
    verify_dependencies_task = PythonOperator(
        task_id='verify_system_dependencies',
        python_callable=verify_dependencies,
        provide_context=True
    )
    
    # Main system initialization task
    system_initialization_task = PythonOperator(
        task_id='comprehensive_system_initialization',
        python_callable=execute_system_initialization,
        provide_context=True
    )
    
    # Post-processing cleanup task
    post_processing_task = PythonOperator(
        task_id='post_processing_cleanup',
        python_callable=post_processing_cleanup,
        provide_context=True
    )
    
    # Define task dependencies
    verify_dependencies_task >> system_initialization_task >> post_processing_task

# Expose the DAG at the module level
comprehensive_research_dag = dag