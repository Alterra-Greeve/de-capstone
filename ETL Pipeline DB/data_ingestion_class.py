import pandas as pd
import os
from airflow.models import Variable
from pymysql import connect
from datetime import datetime, timedelta
class DataIngestion:
    def __init__(self, logical_date):
        self.connection = connect(
            host=Variable.get("DB_HOST"),
            port=3306,
            db="greeve-prod",
            user=Variable.get("DB_USER"),
            password=Variable.get("DB_PASS")
        )

        self.table_list = None
        self.table_df_dict = None
        self.start_datetime_str = None

        start_datetime = datetime.strptime(logical_date, '%Y-%m-%d')
        end_datetime = start_datetime + timedelta(days=1, milliseconds=-1)

        self.date = logical_date
        self.start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
        self.end_datetime_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    def get_data(self):
        cursor = self.connection.cursor()

        last_run_date_str = Variable.get("last_run_date")

        if last_run_date_str == "firstrun":
            # print("First Run!")
            date_condition = f"created_at <= '{self.start_datetime_str}' OR updated_at <= '{self.start_datetime_str}'"
        else:
            # print("Not First Run!")

            date_condition = f"""
            (created_at >= '{self.start_datetime_str}' AND created_at <= '{self.end_datetime_str}')
            OR (updated_at >= '{self.start_datetime_str}' AND updated_at <= '{self.end_datetime_str}')
            """

        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        self.table_list = [table[0] for table in tables]
        self.table_df_dict = dict.fromkeys(self.table_list, None)

        for table in self.table_list:
            query = f"""
            SELECT * FROM {table}
            WHERE {date_condition}
            """
            cursor.execute(query)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            self.table_df_dict[table] = pd.DataFrame(results, columns=column_names)
        self.connection.close()

    def save_data(self, dirname):
        dirname_date = os.path.join(dirname,  f"{self.date}/")
        if not os.path.exists(dirname_date):
            os.makedirs(dirname_date)

        for table in self.table_list:
            self.table_df_dict[table].to_csv(f"{dirname_date}{table}.csv", index=False)

        Variable.set("last_run_date", self.date)

        return dirname_date