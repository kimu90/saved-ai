# expert_update_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import asyncio

# Fix the import paths
from ai_services_api.services.search.indexing.index_creator import ExpertSearchIndexManager
from ai_services_api.services.search.indexing.redis_index_manager import ExpertRedisIndexManager
from ai_services_api.services.recommendation.graph_initializer import GraphDatabaseInitializer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['briankimu97@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

index_processing_dag = DAG(
    'expert_data_processing',
    default_args=default_args,
    description='Process graph, search and Redis indices when experts are updated',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
)

expert_update_sensor = SqlSensor(
    task_id='expert_update_sensor',
    conn_id='postgres_default',
    sql="""
        SELECT COUNT(*)
        FROM experts_expert
        WHERE updated_at >= current_timestamp - interval '5 minutes'
    """,
    poke_interval=300,
    timeout=3600,
    mode='poke',
    dag=index_processing_dag
)

async def initialize_graph(**context):
    graph_initializer = GraphDatabaseInitializer()
    await graph_initializer.initialize_graph()
    context['task_instance'].xcom_push(key='graph_status', value='Graph initialization completed')

def run_graph_init(**context):
    asyncio.run(initialize_graph(**context))

def update_search_indices(**context):
    # Update FAISS index
    faiss_manager = ExpertSearchIndexManager()
    faiss_manager.create_faiss_index()
    context['task_instance'].xcom_push(key='faiss_status', value='FAISS index updated')
    
    # Update Redis index
    redis_manager = ExpertRedisIndexManager()
    redis_manager.clear_redis_indexes()
    redis_manager.create_redis_index()
    context['task_instance'].xcom_push(key='redis_status', value='Redis index updated')

def generate_email_content(**context):
    ti = context['task_instance']
    graph_status = ti.xcom_pull(task_ids='initialize_graph', key='graph_status')
    faiss_status = ti.xcom_pull(task_ids='update_indices', key='faiss_status')
    redis_status = ti.xcom_pull(task_ids='update_indices', key='redis_status')
    
    email_content = f"""
    Expert Data Processing Completed:
    
    - {graph_status}
    - {faiss_status}
    - {redis_status}
    
    Time completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
    context['task_instance'].xcom_push(key='email_content', value=email_content)

# Tasks
graph_init_task = PythonOperator(
    task_id='initialize_graph',
    python_callable=run_graph_init,
    dag=index_processing_dag,
)

index_update_task = PythonOperator(
    task_id='update_indices',
    python_callable=update_search_indices,
    dag=index_processing_dag,
)

email_content_task = PythonOperator(
    task_id='generate_email_content',
    python_callable=generate_email_content,
    provide_context=True,
    dag=index_processing_dag,
)

email_notification = EmailOperator(
    task_id='send_completion_email',
    to=['briankimu97@gmail.com'],
    subject='Expert Data Processing Complete',
    html_content="{{ task_instance.xcom_pull(key='email_content') }}",
    dag=index_processing_dag,
)

expert_update_sensor >> [graph_init_task, index_update_task] >> email_content_task >> email_notification