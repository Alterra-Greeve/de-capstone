import os
from airflow.models import Variable
import csv
from datetime import datetime, timedelta
from google.cloud import logging_v2
import pytz

class DataIngestion:
    def __init__(self):

        self.service_account_path = Variable.get("SERVICE_ACC_CRED")
        self.project_id = Variable.get("PROJECT_ID")
        self.local_tz = pytz.timezone('Asia/Jakarta')

    def get_data(self, start_date, end_date, dirname, log_msg_start, log_msg_end):
        if os.path.exists(self.service_account_path):
            client = logging_v2.Client.from_service_account_json(self.service_account_path)
        else:
            print("There are no service account credentials")
            return

        start_time = self.local_tz.localize(datetime.strptime(start_date, "%Y-%m-%d")).replace(hour=0, minute=0, second=0)
        end_time = self.local_tz.localize(datetime.strptime(end_date, "%Y-%m-%d")).replace(hour=0, minute=0, second=0) - timedelta(milliseconds=1)

        query = f"resource.type=cloud_run_revision AND textPayload:\"{log_msg_start}\""
        resource_names = [f"projects/{self.project_id}"]

        start_time_utc = start_time.astimezone(pytz.UTC).isoformat()
        end_time_utc = end_time.astimezone(pytz.UTC).isoformat()

        filter_query = (
            f"{query} AND timestamp >= \"{start_time_utc}\""
            f" AND timestamp <= \"{end_time_utc}\""
        )

        entries = client.list_entries(filter_=filter_query, resource_names=resource_names)

        if not os.path.exists(dirname):
            os.makedirs(dirname)

        output_file_csv = f"{dirname}logs_{start_date}_to_{end_date}.csv"

        with open(output_file_csv, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            
            writer.writerow(['timestamp', 'textPayload'])
            
            for entry in entries:
                log_entry = entry.to_api_repr()
                timestamp_utc = datetime.strptime(log_entry['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
                timestamp_local = timestamp_utc.replace(tzinfo=pytz.UTC).astimezone(self.local_tz)
                text_payload = log_entry.get('textPayload', '')

                start_index = text_payload.find(log_msg_start)
                end_index = text_payload.find(log_msg_end)

                if start_index != -1 and end_index != -1 and end_index > start_index:
                    
                    extracted_content = text_payload[start_index:end_index + len(log_msg_end)]
                else:
                    extracted_content = text_payload

                writer.writerow([timestamp_local.strftime('%Y-%m-%d %H:%M:%S'), extracted_content])

        print(f"Logs saved to {output_file_csv}")
        return dirname