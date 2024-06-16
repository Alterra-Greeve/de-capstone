import pandas as pd
import os
import csv

class DataTransformer:
    def __init__(self, data_dir, transformed_data_dir):
        self.data_dir = data_dir
        self.transformed_data_dir = transformed_data_dir
        
        # Buat direktori jika belum ada
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        if not os.path.exists(self.transformed_data_dir):
            os.makedirs(self.transformed_data_dir)
            
    def process_data(self, df):
        # Cek Missing Values
        print("Missing values:")
        print(df.isnull().sum())

        # CCek Duplikat Data
        print("Duplicate rows:")
        print(df[df.duplicated()])

        # Drop duplikat data
        df = df.drop_duplicates()

        return df

    def transform_open_product(self):
        open_product = self.df[self.df['textPayload'].str.contains('OPEN-CATEGORY')]
        open_product = self.process_data(open_product)

        transformed_data = []
        for index, row in open_product.iterrows():
            timestamp = row['timestamp']
            log_start = '[LOG_DE_START]'
            user_id = row['textPayload'].split()[1]
            action = row['textPayload'].split()[2]
            product_id = ' '.join(row['textPayload'].split()[3:-1])
            log_end = '[LOG_DE_END]'

            transformed_row = {
                'timestamp': timestamp,
                'log_start': log_start,
                'user_id': user_id,
                'action': action,
                'product_id': product_id,
                'log_end': log_end
            }
            transformed_data.append(transformed_row)

        open_product_df = pd.DataFrame(transformed_data)
        open_product_df = open_product_df.drop(['log_start', 'action', 'log_end'], axis=1)
        open_product_df.to_csv('transformed_data/open_product3.csv', index=False)
        return open_product_df

    def transform_search_product(self):
        search_product = self.df[self.df['textPayload'].str.contains('SEARCH-PRODUCT')]
        search_product = self.process_data(search_product)

        transformed_data = []
        for index, row in search_product.iterrows():
            timestamp = row['timestamp']
            log_start = '[LOG_DE_START]'
            user_id = row['textPayload'].split()[1]
            action = row['textPayload'].split()[2]
            keyword = ' '.join(row['textPayload'].split()[3:-1])
            log_end = '[LOG_DE_END]'

            transformed_row = {
                'timestamp': timestamp,
                'log_start': log_start,
                'user_id': user_id,
                'action': action,
                'keyword': keyword,
                'log_end': log_end
            }
            transformed_data.append(transformed_row)

        search_product_df = pd.DataFrame(transformed_data)
        search_product_df = search_product_df.drop(['log_start', 'action', 'log_end'], axis=1)
        search_product_df.to_csv('transformed_data/search_product3.csv', index=False)
        return search_product_df

    def transform_open_category(self):
        open_category = self.df[self.df['textPayload'].str.contains('OPEN-CATEGORY')]
        open_category = self.process_data(open_category)

        transformed_data = []
        for index, row in open_category.iterrows():
            timestamp = row['timestamp']
            log_start = '[LOG_DE_START]'
            user_id = row['textPayload'].split()[1]
            action = row['textPayload'].split()[2]
            category = ' '.join(row['textPayload'].split()[3:-1])
            log_end = '[LOG_DE_END]'

            transformed_row = {
                'timestamp': timestamp,
                'log_start': log_start,
                'user_id': user_id,
                'action': action,
                'category': category,
                'log_end': log_end
            }
            transformed_data.append(transformed_row)

        open_category_df = pd.DataFrame(transformed_data)
        open_category_df = open_category_df.drop(['log_start', 'action', 'log_end'], axis=1)
        open_category_df.to_csv('transformed_data/open_category3.csv', index=False)
        return open_category_df

        # def transform_data(self):
        #     self.transform_open_product()
        #     self.transform_search_product()
        #     self.transform_open_category()
        #     return self.transformed_data_dir
        
# data_transformer = DataTransformer(data_dir, transformed_data_dir, df)
# transformed_data_dir = data_transformer.transform_data()
# print(f"Data hasil transformasi disimpan di: {transformed_data_dir}")