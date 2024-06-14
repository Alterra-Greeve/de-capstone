from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from data_ingestion_class import DataIngestion
from data_transform_class import DataTransformation
from data_load_class import DataLoad

default_args = {
    'owner': 'DE - Capstone Kelompok 1',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def data_ingestion():
    ingest_obj = DataIngestion()
    ingest_obj.get_data()
    save_directory = "dags/save_ingest/"
    return ingest_obj.save_data(save_directory)

def data_transform(ti):
    directory = ti.xcom_pull(task_ids='data_ingestion')
    print(f"Hi dag dt! {directory}")
    transform_obj = DataTransformation(directory)
    return transform_obj.transform_data()

def data_load(ti):
    directory = ti.xcom_pull(task_ids='data_transform')
    load_obj = DataLoad(directory)

with DAG(
    dag_id="capstone",
    default_args=default_args,
    description="DAG for Greeve",
    start_date=datetime(2024, 6, 12),
    schedule_interval="@daily"
) as dag:

    data_ingestion_task = PythonOperator(
        task_id="data_ingestion",
        python_callable=data_ingestion
    )

    data_transform_task = PythonOperator(
        task_id="data_transform",
        python_callable=data_transform
    )

    data_load_task = PythonOperator(
        task_id="data_load",
        python_callable=data_load
    )

    data_ingestion_task  >> data_transform_task >> data_load_task