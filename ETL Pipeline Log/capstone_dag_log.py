from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pendulum
from airflow.decorators import dag, task
from data_ingestion_class_log import DataIngestion
import pytz
from data_transform_class_logs import DataTransformation
from data_load_class_log import DataLoad

local_tz = pytz.timezone("Asia/Jakarta")
start_airflow_date = pendulum.datetime(2024, 6, 13, tz="Asia/Jakarta")

default_args = {
    'owner': 'DE - Capstone Kelompok 1',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def data_ingestion(logical_date, **kwargs):
    log_msg_start = "[LOG_DE_START]"
    log_msg_end = "[LOG_DE_END]"
    dirname="dags/de_logs/"

    print(logical_date)
    logical_date_gmt7 = pendulum.instance(logical_date).in_timezone(local_tz)
    print(logical_date_gmt7)

    start_date = logical_date_gmt7.strftime("%Y-%m-%d")
    end_date = (logical_date_gmt7 + timedelta(days=1)).strftime("%Y-%m-%d")

    ingest_obj = DataIngestion()
    return ingest_obj.get_data(start_date, end_date, dirname, log_msg_start, log_msg_end)

def data_transform(logical_date, **kwargs):
    ti = kwargs['ti']
    output_dir = ti.xcom_pull(task_ids='data_ingestion')
    logical_date_gmt7 = pendulum.instance(logical_date).in_timezone(local_tz)
    now_date = logical_date_gmt7.strftime("%Y-%m-%d")
    
    if not output_dir:
        raise ValueError("XCom returned None or empty output_dir from data_ingestion_task.")

    print(f"Received output_dir: {output_dir}")

    transform_obj = DataTransformation(output_dir,now_date)
    transformed_data_dir = transform_obj.transform_data()

    return transformed_data_dir

def data_load(logical_date, **kwargs):
    ti = kwargs['ti']
    directory = ti.xcom_pull(task_ids='data_transform')
    
    logical_date_gmt7 = pendulum.instance(logical_date).in_timezone(local_tz)
    now_date = logical_date_gmt7.strftime("%Y-%m-%d")
    
    DataLoad(directory,now_date)

# f"{outputfile_csv}logs{logicaldate}"

with DAG(
    dag_id="Capstone-Log-Pipeline",
    default_args=default_args,
    description="DAG for Greeve",
    start_date=start_airflow_date,
    schedule_interval="@daily"
) as dag:

    data_ingestion_task = PythonOperator(
        task_id="data_ingestion",
        python_callable=data_ingestion,
        provide_context=True
    )

    data_transform_task = PythonOperator(
        task_id="data_transform",
        python_callable=data_transform,
        provide_context=True
    )

    data_load_task = PythonOperator(
        task_id="data_load",
        python_callable=data_load,
        provide_context=True
    )

    data_ingestion_task >> data_transform_task >> data_load_task
    
