import sys
import os
import datetime
import psycopg2
from sqlalchemy import create_engine
import shutil
import pandas as pd
# Import the 'config' funtion from the config.py file
from config import config

class Tests:
  def __init__(self, sql, csv):
    self.sql = sql
    self.csv = csv

try:
    date = sys.argv[1]
except:
    date = datetime.datetime.now().strftime("%Y-%m-%d")

has_succeed = Tests(True, False)
# Obtain the configuration parameters
params_local_db = config("ini/local_db.ini")
params_cloud_db = config("ini/cloud_db.ini")
# Connect to the PostgreSQL database
conn_local_db = psycopg2.connect(**params_local_db)
engine = create_engine(params_cloud_db['url'], echo = False)

def validate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("date format should be YYYY-MM-DD")

def extract_all_tables():
    query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
    tables_list = pd.read_sql_query(query, conn_local_db).values.tolist()    
    return tables_list

# A function that takes in a PostgreSQL query and outputs a pandas database 
def create_pandas_table(table, date, database = conn_local_db):
    try:
        export_table = pd.read_sql_query(f"SELECT * FROM {table}", database).to_csv(index=False)
        
        folder_path = f'data/postgresql/{table}/{date}'

        if os.path.exists(folder_path):
            file = f'{folder_path}/export.csv'
        else:
            os.makedirs(folder_path, mode=0o770)
            file = f'{folder_path}/export.csv'                
        
        f=open(file, mode='w', encoding='utf-8')
        f.write(export_table)
        f.close()
        # return print(f'table "{table}" done')
        return True
    except:
        # return print(f'error create_pandas_table, table: "{table}"')
        return False

def local_write_csv(date):
    try:
        folder_path = f'data/csv/{date}'
        if os.path.exists(folder_path):
            file = f'{folder_path}/export.csv'
        else:
            os.makedirs(folder_path, mode=0o770)
            file = f'{folder_path}/export.csv'
        shutil.copy('data/order_details.csv', file)
        # return print(f'table local .csv done') 
        return True
    except:
        # return print("error local_write_csv")
        return False

def write_cloud_db(has_succeed, date):
    try:
        if has_succeed.csv & has_succeed.sql:
            orders_df = pd.read_csv(f'data/postgresql/orders/{date}/export.csv')
            orders_details_df = pd.read_csv(f'data/csv/{date}/export.csv')
            
            # since my database is hosted at heroku free tier, if_exists='replace'
            orders_df.to_sql('orders', con=engine, if_exists='replace')
            orders_details_df.to_sql('order_details', con=engine,if_exists='replace')
            return print("done write_cloud_db")
        else:
            return print("do not run for a day")
    except TypeError as error:
        return print("error write_cloud_db " + error)

validate(date)

tables_list = extract_all_tables()

for index in range(len(tables_list)):
    has_succeed.sql = has_succeed.sql & create_pandas_table(tables_list[index][0], date)
    
has_succeed.csv = local_write_csv(date)

conn_local_db.close()

write_cloud_db(has_succeed, date)