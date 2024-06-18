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
    
    def process_and_save_data(self):
        # Read the original CSV data
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
        
        # Save processed data to CSV
        processed_data_file = os.path.join(self.transformed_data_dir, "processed_data.csv")
        df.to_csv(processed_data_file, index=False)
        
        # Separate data into different DataFrames and save each to CSV
        open_category_df = df[df['doing'] == 'OPEN-CATEGORY'].drop(columns=['doing'])
        open_product_df = df[df['doing'] == 'OPEN-PRODUCT'].drop(columns=['doing']).rename(columns={'category': 'product_id'})
        search_product_df = df[df['doing'] == 'SEARCH-PRODUCT'].drop(columns=['doing']).rename(columns={'category': 'keyword'})
        
        open_category_file = os.path.join(self.transformed_data_dir, "open_category.csv")
        open_product_file = os.path.join(self.transformed_data_dir, "open_product.csv")
        search_product_file = os.path.join(self.transformed_data_dir, "search_product.csv")
        
        open_category_df.to_csv(open_category_file, index=False)
        open_product_df.to_csv(open_product_file, index=False)
        search_product_df.to_csv(search_product_file, index=False)
        
        return processed_data_file, open_category_file, open_product_file, search_product_file
