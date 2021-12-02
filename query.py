# import sys
import datetime
from sqlalchemy import create_engine
import pandas as pd
import numpy as nb
# Import the 'config' funtion from the config.py file
from config import config



suffix = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
file = f'data/step2-{suffix}.csv'

params_cloud_db = config("ini/cloud_db.ini")
# Connect to the PostgreSQL database
engine = create_engine(params_cloud_db['url'], echo = False)

export_table = pd.read_sql_query(f"SELECT * FROM order_details RIGHT JOIN orders ON order_details.order_id = orders.order_id", engine).to_csv(index=False)

f=open(file, mode='w', encoding='utf-8')
f.write(export_table)
f.close()