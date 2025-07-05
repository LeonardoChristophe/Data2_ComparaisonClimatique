from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from scripts.extract import save_raw_data
from scripts.merge import merge_data
from scripts.transform import create_star_schema

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_comparison_pipeline',
    default_args=default_args,
    description='Un pipeline ETL pour comparer les données météo entre villes',
    schedule_interval=timedelta(days=1),
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=save_raw_data,
    dag=dag
)

merge_task = PythonOperator(
    task_id='merge_weather_data',
    python_callable=merge_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_to_star_schema',
    python_callable=create_star_schema,
    dag=dag
)

extract_task >> merge_task >> transform_task