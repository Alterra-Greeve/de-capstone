import os
from datetime import datetime
from firebase_admin import credentials, initialize_app, storage
from airflow.models import Variable

def DataLoad(folder_path):
    try:
        # Load Firebase credentials path and Firebase Storage bucket from Airflow Variables
        CERTIFICATE_PATH = Variable.get("firebase_certificate_path")
        STORAGE_BUCKET = Variable.get("firebase_storage_bucket")

        # Initialize Firebase app if not already initialized
        cred = credentials.Certificate(CERTIFICATE_PATH)
        initialize_app(cred, {"storageBucket": STORAGE_BUCKET}, name="firebase_storage")

        # Get a reference to the Firebase Storage service
        bucket = storage.bucket(app=storage.app(name="firebase_storage"))

        current_date = datetime.now().strftime("%Y-%m-%d")

        # Get all CSV files in the specified folder
        csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

        for csv_file in csv_files:
            file_path = os.path.join(folder_path, csv_file)

            # Ensure it's a file and not a directory
            if os.path.isfile(file_path):
                # Upload file to Firebase Storage
                blob = bucket.blob(f"{current_date}/{csv_file}")
                blob.upload_from_filename(file_path)

                print(f"Data from {csv_file} loaded successfully")

    except Exception as e:
        print(f"An error occurred when loading data to Firebase Storage: {e}")
