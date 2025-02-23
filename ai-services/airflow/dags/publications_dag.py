# publications_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import asyncio

from ai_services_api.services.centralized_repository.openalex.openalex_processor import OpenAlexProcessor
from ai_services_api.services.centralized_repository.publication_processor import PublicationProcessor
from ai_services_api.services.centralized_repository.ai_summarizer import TextSummarizer
from ai_services_api.services.centralized_repository.orcid.orcid_processor import OrcidProcessor
from ai_services_api.services.centralized_repository.knowhub.knowhub_scraper import KnowhubScraper
from ai_services_api.services.centralized_repository.website.website_scraper import WebsiteScraper
from ai_services_api.services.centralized_repository.nexus.researchnexus_scraper import ResearchNexusScraper

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['briankimu97@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

publications_dag = DAG(
    'publications_processing',
    default_args=default_args,
    description='Process publications from various sources monthly',
    schedule_interval='0 0 1 * *',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

async def process_publications(**context):
    openalex_processor = OpenAlexProcessor()
    publication_processor = PublicationProcessor(openalex_processor.db, TextSummarizer())
    publication_stats = {
        'openalex': 0,
        'orcid': 0,
        'knowhub': 0,
        'researchnexus': 0,
        'website': 0
    }
    
    try:
        # Process OpenAlex publications
        try:
            await openalex_processor.process_publications(publication_processor, source='openalex')
            publication_stats['openalex'] = await openalex_processor.get_processed_count()
        except Exception as e:
            context['task_instance'].xcom_push(key='openalex_error', value=str(e))

        # Process ORCID publications
        try:
            orcid_processor = OrcidProcessor()
            await orcid_processor.process_publications(publication_processor, source='orcid')
            publication_stats['orcid'] = await orcid_processor.get_processed_count()
        except Exception as e:
            context['task_instance'].xcom_push(key='orcid_error', value=str(e))

        # Process other sources...
        context['task_instance'].xcom_push(key='publication_stats', value=publication_stats)
        
    finally:
        # Cleanup
        openalex_processor.close()
        if 'orcid_processor' in locals(): orcid_processor.close()

def run_async_publications(**context):
    asyncio.run(process_publications(**context))

def generate_publications_email(**context):
    ti = context['task_instance']
    stats = ti.xcom_pull(key='publication_stats')
    errors = {
        'openalex': ti.xcom_pull(key='openalex_error'),
        'orcid': ti.xcom_pull(key='orcid_error'),
        'knowhub': ti.xcom_pull(key='knowhub_error'),
        'researchnexus': ti.xcom_pull(key='researchnexus_error'),
        'website': ti.xcom_pull(key='website_error')
    }
    
    email_content = f"""
    Publications Processing Report ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
    
    Publications Processed:
    - OpenAlex: {stats['openalex']} publications
    - ORCID: {stats['orcid']} publications
    - KnowHub: {stats['knowhub']} items
    - ResearchNexus: {stats['researchnexus']} publications
    - Website: {stats['website']} publications
    
    Total: {sum(stats.values())} items processed
    """
    
    if any(errors.values()):
        email_content += "\nErrors Encountered:\n"
        for source, error in errors.items():
            if error:
                email_content += f"- {source}: {error}\n"
    
    context['task_instance'].xcom_push(key='email_content', value=email_content)

publications_task = PythonOperator(
    task_id='process_publications',
    python_callable=run_async_publications,
    provide_context=True,
    dag=publications_dag,
)

email_content_task = PythonOperator(
    task_id='generate_email_content',
    python_callable=generate_publications_email,
    provide_context=True,
    dag=publications_dag,
)

email_notification = EmailOperator(
    task_id='send_completion_email',
    to=['briankimu97@gmail.com'],
    subject='Publications Processing Complete',
    html_content="{{ task_instance.xcom_pull(key='email_content') }}",
    dag=publications_dag,
)

publications_task >> email_content_task >> email_notification