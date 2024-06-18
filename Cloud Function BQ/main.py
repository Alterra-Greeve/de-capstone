import logging
import os
import traceback
import re

from google.cloud import bigquery
from google.cloud import storage

import yaml

import functions_framework

with open("./schemas.yaml") as schema_file:
    config = yaml.load(schema_file, Loader=yaml.Loader)

PROJECT_ID = os.getenv('alterra-playground.appspot.com')
BQ_DATASET = 'GreeveDimension'
CS = storage.Client()
BQ = bigquery.Client()
job_config = bigquery.LoadJobConfig()

def streaming(data):
    bucketname = data['bucket'] 
    print("Bucket name",bucketname)
    filename = data['name']   
    print("File name",filename)  
    timeCreated = data['timeCreated']
    print("Time Created",timeCreated) 

    try:
        match = re.match(r'(\d{4}-\d{2}-\d{2})/([^/]+\.csv)$', filename)
        if not match:
            print("Filename does not match the expected pattern")
            return
        
        folder_date, file_name = match.groups()
        print("Folder date:", folder_date)
        print("Extracted filename:", file_name)

        for table in config:
            tableName = table.get('name')
  
            if re.search(tableName.replace('_', '-'), file_name) or re.search(tableName, file_name):
                tableSchema = table.get('schema')
                tableFormat = table.get('format')

                _check_if_table_exists(tableName, tableSchema)

                staging_table_name = f"{tableName}_staging"
                _load_table_from_uri(bucketname, filename, tableSchema, staging_table_name)

                _merge_into_final_table(staging_table_name, tableName, tableSchema)

                _delete_staging_table(staging_table_name)

    except Exception:
        print('Error streaming file {filename}. Cause: %s' % (traceback.format_exc()))

def _check_if_table_exists(tableName, tableSchema):
    table_id = BQ.dataset(BQ_DATASET).table(tableName)

    try:
        BQ.get_table(table_id)
    except Exception:
        logging.warning('Creating table: %s' % (tableName))
        schema = create_schema_from_yaml(tableSchema)

        partitioning_field = None
        for table in config:
            if table['name'] == tableName:
                partitioning_field = table.get('partitioning_field')
                break

        table = bigquery.Table(table_id, schema=schema)
        
        if partitioning_field:
            table.time_partitioning = bigquery.TimePartitioning(
                field=partitioning_field,
                type_=bigquery.TimePartitioningType.MONTH  # You can choose DAY, MONTH, YEAR
            )
            print(f"Partitioning field set to {partitioning_field}")

        try:
            table = BQ.create_table(table)
            print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
        except Exception as create_err:
            print(f"Failed to create table {table_name}. Reason: {str(create_err)}")


def _load_table_from_uri(bucket_name, file_name, tableSchema, staging_table_name):
    uri = 'gs://%s/%s' % (bucket_name, file_name)
    staging_table_id = BQ.dataset(BQ_DATASET).table(staging_table_name)

    schema = create_schema_from_yaml(tableSchema)
    print(schema)
    job_config.schema = schema

    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 1

    try:
        load_job = BQ.load_table_from_uri(uri, staging_table_id, job_config=job_config)
        load_job.result()
        print(f"Loaded data into staging table: {staging_table_name}")
    except Exception as load_err:
        print(f"Failed to load data into staging table {staging_table_name}. Reason: {str(load_err)}")

def _merge_into_final_table(staging_table_name, final_table_name, tableSchema):
    staging_table_id = f"{BQ_DATASET}.{staging_table_name}"
    final_table_id = f"{BQ_DATASET}.{final_table_name}"
    primary_key = f"{final_table_name}_id"

    partitioning_field = None
    for table in config:
        if table['name'] == final_table_name:
            partitioning_field = table.get('partitioning_field')
            break

    partition_filter = f"AND T.{partitioning_field} = S.{partitioning_field}" if partitioning_field else ""

    merge_query = f"""
        MERGE `{final_table_id}` T
        USING `{staging_table_id}` S
        ON T.{primary_key} = S.{primary_key} {partition_filter}
        WHEN MATCHED THEN
            UPDATE SET
                {', '.join([f"T.{col['name']} = S.{col['name']}" for col in tableSchema if col['name'] != primary_key])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join([col['name'] for col in tableSchema])})
            VALUES ({', '.join([f"S.{col['name']}" for col in tableSchema])});
    """
    
    print("Running merge query:", merge_query)
    try:
        query_job = BQ.query(merge_query)
        query_job.result()
        print(f"Merged data into final table: {final_table_name}")
    except Exception as merge_err:
        print(f"Failed to merge data into final table {final_table_name}. Reason: {str(merge_err)}")


def _delete_staging_table(staging_table_name):
    staging_table_id = BQ.dataset(BQ_DATASET).table(staging_table_name)
    try:
        BQ.delete_table(staging_table_id, not_found_ok=True)
        print(f"Deleted staging table: {staging_table_name}")
    except Exception as delete_err:
        print(f"Failed to delete staging table {staging_table_name}. Reason: {str(delete_err)}")

def create_schema_from_yaml(table_schema):
    schema = []
    for column in table_schema:
        schemaField = bigquery.SchemaField(column['name'], column['type'], column['mode'])
        schema.append(schemaField)

    return schema

def manage_challenge_fact_table():
    try:
        project_id = "alterra-greeve" 
        dataset_id = "GreeveDimension"
        final_table_name = 'challenge_fact_table'
        final_table_id = f"{project_id}.{dataset_id}.{final_table_name}" 
        temp_table_name = 'challenge_fact_table_temp'
        temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"

        bq_client = bigquery.Client()

        schema = [
            bigquery.SchemaField("challenge_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("impact_categories_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("challenge_confirmations_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("confirmation_upload_date", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("challenge_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("category_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("impact_point", "INTEGER", mode="NULLABLE")
        ]

        table_exists = False
        try:
            bq_client.get_table(final_table_id)
            table_exists = True
            print(f"Table {final_table_id} already exists.")
        except Exception as e:
            print(f"Table {final_table_id} does not exist. Attempting to create it.")

        if not table_exists:
            final_table = bigquery.Table(final_table_id, schema=schema)
            try:
                bq_client.create_table(final_table)
                print(f"Created table {final_table_id}")
            except Exception as create_err:
                print(f"Failed to create table {final_table_id}. Reason: {str(create_err)}")
                return

        try:
            bq_client.delete_table(temp_table_id, not_found_ok=True)
            print(f"Deleted existing temporary table {temp_table_id} (if it existed).")
        except Exception as e:
            print(f"Failed to delete existing temporary table {temp_table_id}. Reason: {str(e)}")

        temp_table = bigquery.Table(temp_table_id, schema=schema)
        try:
            bq_client.create_table(temp_table)
            print(f"Created temporary table {temp_table_id}")
        except Exception as create_err:
            print(f"Failed to create temporary table {temp_table_id}. Reason: {str(create_err)}")
            return

        insert_query = f"""
            INSERT INTO `{temp_table_id}` (challenge_id, user_id, impact_categories_id,
                                           challenge_confirmations_status, confirmation_upload_date,
                                           challenge_name, category_name, impact_point)
            SELECT
                cl.challenge_id AS challenge_id,
                cl.user_id AS user_id,
                ic.impact_categories_id as impact_categories_id,
                CASE 
                    WHEN cl.status = 'Ditolak' THEN 'Challenge Tidak Diambil'
                    ELSE cc.status
                END AS challenge_confirmations_status,
                cc.date_upload AS confirmation_upload_date,
                c.title AS challenge_name,
                ic.name AS category_name,
                ic.impact_point AS impact_point
            FROM
                `alterra-greeve.GreeveDimension.challenge_logs` cl
            LEFT JOIN
                `alterra-greeve.GreeveDimension.challenge_confirmations` cc ON cl.challenge_id = cc.challenge_id AND cl.user_id = cc.user_id
            LEFT JOIN
                `alterra-greeve.GreeveDimension.challenges` c ON cl.challenge_id = c.challenges_id
            LEFT JOIN
                `alterra-greeve.GreeveDimension.challenge_impact_categories` cic ON cl.challenge_id = cic.challenge_id
            LEFT JOIN
                `alterra-greeve.GreeveDimension.impact_categories` ic ON cic.impact_category_id = ic.impact_categories_id
        """

        print("Running insert query into temporary table:", insert_query)
        try:
            query_job = bq_client.query(insert_query)
            query_job.result()
            print(f"Inserted data into temporary table {temp_table_name}")
        except Exception as insert_err:
            print(f"Failed to insert data into temporary table {temp_table_name}. Reason: {str(insert_err)}")
            return

        overwrite_query = f"""
            CREATE OR REPLACE TABLE `{final_table_id}` AS
            SELECT * FROM `{temp_table_id}`
        """

        print("Running overwrite query:", overwrite_query)
        try:
            query_job = bq_client.query(overwrite_query)
            query_job.result()
            print(f"Overwritten data in table {final_table_name} with data from {temp_table_name}")
        except Exception as overwrite_err:
            print(f"Failed to overwrite data in table {final_table_name}. Reason: {str(overwrite_err)}")
            return

    except Exception as e:
        print(f"Error managing challenge_fact_table: {str(e)}")


@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    streaming(data)

    manage_challenge_fact_table()