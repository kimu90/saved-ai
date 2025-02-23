# web_content_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import asyncio

from ai_services_api.services.centralized_repository.web_content.services.processor import WebContentProcessor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['briankimu97@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

web_content_dag = DAG(
    'web_content_processing',
    default_args=default_args,
    description='Process web content monthly',
    schedule_interval='0 0 1 * *',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

async def process_web_content(**context):
    processor = WebContentProcessor(max_workers=4)
    try:
        results = await processor.process_content()
        context['task_instance'].xcom_push(key='processing_results', value=results)
    finally:
        await processor.cleanup()

def run_async_web_content(**context):
    asyncio.run(process_web_content(**context))

def generate_web_content_email(**context):
    ti = context['task_instance']
    results = ti.xcom_pull(key='processing_results')
    
    email_content = f"""
    Web Content Processing Report ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
    
    Processing Results:
    - Pages Processed: {results['processed_pages']}
    - Pages Updated: {results['updated_pages']}
    - PDF Chunks Processed: {results['processed_chunks']}
    - PDF Chunks Updated: {results['updated_chunks']}
    
    Processing Time: {results.get('processing_time', 'N/A')} seconds
    Average Time Per Page: {results.get('avg_time_per_page', 'N/A')} seconds
    """
    context['task_instance'].xcom_push(key='email_content', value=email_content)

web_content_task = PythonOperator(
    task_id='process_web_content',
    python_callable=run_async_web_content,
    provide_context=True,
    dag=web_content_dag,
)

email_content_task = PythonOperator(
    task_id='generate_email_content',
    python_callable=generate_web_content_email,
    provide_context=True,
    dag=web_content_dag,
)

email_notification = EmailOperator(
    task_id='send_completion_email',
    to=['briankimu97@gmail.com'],
    subject='Web Content Processing Complete',
    html_content="{{ task_instance.xcom_pull(key='email_content') }}",
    dag=web_content_dag,
)

web_content_task >> email_content_task >> email_notification