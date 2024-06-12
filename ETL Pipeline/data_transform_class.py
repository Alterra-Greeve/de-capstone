import pandas as pd
import os

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
                
                transformed_file_path = os.path.join(self.transformed_data_dir, file)
                df.to_csv(transformed_file_path, index=False)
                print(f"Transformed data for table {table_name} saved to {transformed_file_path}")