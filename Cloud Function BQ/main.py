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
BQ_DATASET = 'GreeveBackup'
CS = storage.Client()
BQ = bigquery.Client()
job_config = bigquery.LoadJobConfig()

def streaming(data):
    bucketname = data['bucket']
    print("Bucket name", bucketname)
    filename = data['name']
    print("File name", filename)
    timeCreated = data['timeCreated']
    print("Time Created", timeCreated)

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
        print('Error streaming file. Cause: %s' % (traceback.format_exc()))

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
                type_=bigquery.TimePartitioningType.MONTH
            )
            print(f"Partitioning field set to {partitioning_field}")

        table = BQ.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

def _load_table_from_uri(bucket_name, file_name, tableSchema, staging_table_name):
    uri = 'gs://%s/%s' % (bucket_name, file_name)
    staging_table_id = BQ.dataset(BQ_DATASET).table(staging_table_name)

    schema = create_schema_from_yaml(tableSchema)
    print(schema)
    job_config.schema = schema

    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = 1

    load_job = BQ.load_table_from_uri(
        uri,
        staging_table_id,
        job_config=job_config,
    )

    load_job.result()
    print("Loaded data into staging table:", staging_table_name)

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
    query_job = BQ.query(merge_query)
    query_job.result()
    print("Merged data into final table:", final_table_name)

def _delete_staging_table(staging_table_name):
    staging_table_id = BQ.dataset(BQ_DATASET).table(staging_table_name)
    BQ.delete_table(staging_table_id, not_found_ok=True)
    print("Deleted staging table:", staging_table_name)

def create_schema_from_yaml(table_schema):
    schema = []
    for column in table_schema:
        schemaField = bigquery.SchemaField(column['name'], column['type'], column['mode'])
        schema.append(schemaField)

    return schema

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