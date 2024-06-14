import pandas as pd
import os
from airflow.models import Variable
from pymysql import connect

class DataIngestion:
    def __init__(self):
        self.connection = connect(
            host = Variable.get("DB_HOST"),
            port = 3306,
            db = "greeve-prod",
            user = Variable.get("DB_USER"),
            password = Variable.get("DB_PASS")
        )

        self.table_list = None
        self.table_df_dict = None

    def get_data(self):
        cursor = self.connection.cursor()

        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        self.table_list = [table[0] for table in tables]
        self.table_df_dict = dict.fromkeys(self.table_list, None)

        for table in self.table_list:
            cursor.execute(f"SELECT * FROM {table}")
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            self.table_df_dict[table] = pd.DataFrame(results, columns=column_names)
        self.connection.close()

    def save_data(self, dirname):
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        for table in self.table_list:
            self.table_df_dict[table].to_csv(f"{dirname}{table}.csv", index=False)

        return dirname