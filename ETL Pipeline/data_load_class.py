import os
import firebase_admin
from firebase_admin import credentials, storage

class DataLoad:
    def __init__(self, firebase_credentials_path, bucket_name):
        # Initialize Firebase Admin SDK
        cred = credentials.Certificate(firebase_credentials_path)
        firebase_admin.initialize_app(cred, {
            'storageBucket': bucket_name
        })
        self.bucket = storage.bucket()

    def upload_files(self, folder_path):
        # Iterate over all CSV files in the folder
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                blob = self.bucket.blob(filename)
                blob.upload_from_filename(file_path)
                print(f"File {file_path} uploaded to {self.bucket.name}/{filename}.")
