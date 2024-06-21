import pandas as pd
import os

class DataTransformation:
    def __init__(self, input_dir, execution_date):
        self.execution_date=execution_date
        self.input_file_csv = f"{input_dir}logs_{execution_date}.csv"
        self.transformed_data_dir = os.path.join(input_dir, f"transformed/{execution_date}")
        if not os.path.exists(self.transformed_data_dir):
            os.makedirs(self.transformed_data_dir)

    def transform_data(self):
        # Load the CSV file into a DataFrame
        df = pd.read_csv(self.input_file_csv)

        # Apply transformations to the DataFrame
        df['textPayload'] = [x[x.find(']')+1:x.rfind('[')] for x in df['textPayload']]

        df[['user_id', 'doing', 'category']] = df['textPayload'].str.split(n=2, expand=True)

        df['user_id'] = df['user_id'].str.strip()
        df['doing'] = df['doing'].str.strip()
        df['category'] = df['category'].str.strip()

        df.drop(columns=['textPayload'], inplace=True)

        open_category_df = df[df['doing'] == 'OPEN-CATEGORY'].drop(columns=['doing'])
        open_product_df = df[df['doing'] == 'OPEN-PRODUCT'].drop(columns=['doing'])
        search_product_df = df[df['doing'] == 'SEARCH-PRODUCT'].drop(columns=['doing']) 

        open_product_df = open_product_df.rename(columns={'category': 'product_id'})
        search_product_df = search_product_df.rename(columns={'category': 'keyword'})

        # Save transformed DataFrames to new CSV files
        transformed_open_category = os.path.join(self.transformed_data_dir, "transformed_open_category.csv")
        open_category_df.to_csv(transformed_open_category, index=False)

        transformed_open_product = os.path.join(self.transformed_data_dir, "transformed_open_product.csv")
        open_product_df.to_csv(transformed_open_product, index=False)

        transformed_search_product = os.path.join(self.transformed_data_dir, "transformed_search_product.csv")
        search_product_df.to_csv(transformed_search_product, index=False)

        return self.transformed_data_dir