import pandas as pd
import os
class DataTransformation:
    
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.transformed_data_dir = os.path.join(self.data_dir, "transformed")
        if not os.path.exists(self.transformed_data_dir):
            os.makedirs(self.transformed_data_dir)

    def clean_text(self, text):
        text = text.replace('[LOG_DE_START]', '')
        text = text.replace('[LOG_DE_END]', '')
        return text
    
    def process_data(self):
        df = pd.read_csv(self.data_dir)
        
        # Clean textPayload column
        df['textPayload'] = df['textPayload'].apply(self.clean_text)
        
        # Split textPayload into user_id, doing, and category
        df[['user_id', 'doing', 'category']] = df['textPayload'].str.split(n=2, expand=True)
        
        # Strip extra spaces
        df['user_id'] = df['user_id'].str.strip()
        df['doing'] = df['doing'].str.strip()
        df['category'] = df['category'].str.strip()
        
        # Drop textPayload column
        df.drop(columns=['textPayload'], inplace=True)
        
        return df

    def separate_dataframes(self, processed_df):
        open_category_df = processed_df[processed_df['doing'] == 'OPEN-CATEGORY'].drop(columns=['doing'])
        open_product_df = processed_df[processed_df['doing'] == 'OPEN-PRODUCT'].drop(columns=['doing'])
        search_product_df = processed_df[processed_df['doing'] == 'SEARCH-PRODUCT'].drop(columns=['doing'])
        
        open_product_df = open_product_df.rename(columns={'category': 'product_id'})
        search_product_df = search_product_df.rename(columns={'category': 'keyword'})
        
        return open_category_df, open_product_df, search_product_df
