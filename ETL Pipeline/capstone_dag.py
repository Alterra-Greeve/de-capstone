from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
from airflow.decorators import dag, task
# from data_ingestion_class import DataIngestion
# from data_transform_class import DataTransformation
from data_load_class import DataLoad
import pendulum
import pytz

local_tz = pytz.timezone("Asia/Jakarta")
start_airflow_date = pendulum.datetime(2024, 6, 15, tz="Asia/Jakarta")

default_args = {
    'owner': 'DE - Capstone Kelompok 1',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# def data_ingestion(logical_date, **kwargs):

#     execution_date_gmt7 = pendulum.instance(logical_date).in_timezone(local_tz)
#     date = execution_date_gmt7.strftime("%Y-%m-%d")

#     save_directory = "dags/save_ingest/"
    
#     ingest_obj = DataIngestion(date)
#     ingest_obj.get_data()

#     return ingest_obj.save_data(save_directory)

# def data_transform(ti, **kwargs):

#     directory = ti.xcom_pull(task_ids='data_ingestion')

#     transform_obj = DataTransformation(directory)
#     return transform_obj.transform_data()

def data_load(ti, logical_date, **kwargs):

    directory = ti.xcom_pull(task_ids='data_transform')
    execution_date_gmt7 = pendulum.instance(logical_date).in_timezone(local_tz)
    date = execution_date_gmt7.strftime("%Y-%m-%d")

    load_obj = DataLoad(directory, date)

with DAG(
    dag_id="capstone",
    default_args=default_args,
    description="DAG for Greeve",
    start_date=start_airflow_date,
    schedule_interval="@daily",
    max_active_runs=1,
    concurrency=1
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

    # data_ingestion_task  >> data_transform_task >> 
    data_load_task