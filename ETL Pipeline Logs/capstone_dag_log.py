from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from data_ingestion_class_log import DataIngestion
import pytz
from data_transform_class_logs import DataTransformation
# from data_load_class_log import DataLoad

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
    dirname="dags/logs/"

    print(logical_date)
    execution_date_gmt7 = pendulum.instance(logical_date).in_timezone(local_tz)
    print(execution_date_gmt7)

    start_date = execution_date_gmt7.strftime("%Y-%m-%d")
    end_date = (execution_date_gmt7 + timedelta(days=1)).strftime("%Y-%m-%d")

    ingest_obj = DataIngestion()
    return ingest_obj.get_data(start_date, end_date, dirname, log_msg_start, log_msg_end)

def data_transform(ti, **kwargs):
    directory = ti.xcom_pull(task_ids='data_ingestion')
    print(f"Hi dag dt! {directory}")
    transform_obj = DataTransformation(directory)
    return transform_obj.transform_data()

# def data_load(ti):
#     directory = ti.xcom_pull(task_ids='data_transform')
#     load_obj = DataLoad(directory)

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

    # data_load_task = PythonOperator(
    #     task_id="data_load",
    #     python_callable=data_load
    # )

    # data_ingestion_task  >> data_transform_task >> data_load_task
    data_ingestion_task >> data_transform_task