import pandas as pd
import os
from datetime import datetime

class DataTransformation:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.transformed_data_dir = os.path.join(self.data_dir, "transformed")
        if not os.path.exists(self.transformed_data_dir):
            os.makedirs(self.transformed_data_dir)

    def transform_data(self):
        for file in os.listdir(self.data_dir):
            if file.endswith(".csv"):
                table_name = os.path.splitext(file)[0]
                df = pd.read_csv(os.path.join(self.data_dir, file))
                
                # Cek nilai NULL
                null_count = df.isnull().sum()
                print(f"Jumlah nilai NULL untuk tabel {table_name}:")
                print(null_count)
                
                # Cek duplikat data
                duplicate_count = df.duplicated().sum()
                print(f"Jumlah data duplikat untuk tabel {table_name}: {duplicate_count}")
                
                # Hapus duplikat data
                df.drop_duplicates(inplace=True)
                
                # Ubah nama kolom 'id' menjadi '{table_name}_id'
                df = df.rename(columns={'id': f"{table_name}_id"})
                
                # Ubah format tanggal ke '%Y-%m-%d %H:%M:%S'
                datetime_col = ["date_start", "date_end", "date_upload", "created_at", "updated_at", "deleted_at"]
                for col in datetime_col:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], format='ISO8601')
                        df[col] = df[col].dt.floor('s')
                
                transformed_file_path = os.path.join(self.transformed_data_dir, file)
                df.to_csv(transformed_file_path, index=False)
                print(f"Transformed data for table {table_name} saved to {transformed_file_path}")

        return self.transformed_data_dir