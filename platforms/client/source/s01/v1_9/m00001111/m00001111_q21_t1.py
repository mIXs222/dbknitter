from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd

# MySql connection
mysql_username = 'root'
mysql_password = 'my-secret-pw'
mysql_host = 'mysql'
mysql_database = 'tpch'
mysql_engine = create_engine(f'mysql+pymysql://{mysql_username}:{mysql_password}@{mysql_host}/{mysql_database}')

# Get data from MySql
query = 'SELECT * FROM SUPPLIER'
supplier = pd.read_sql(query, mysql_engine)

query = 'SELECT * FROM NATION'
nation = pd.read_sql(query, mysql_engine)

# MongoDB connection
mongodb_host = 'mongodb'
mongodb_port = '27017'
mongodb_database = 'tpch'
client = MongoClient(f'mongodb://{mongodb_host}:{mongodb_port}/')
db = client[mongodb_database]

# Get data from MongoDB
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# Create a flag in lineitem if L_RECEIPTDATE > L_COMMITDATE
lineitem['flag'] = lineitem.L_RECEIPTDATE > lineitem.L_COMMITDATE

# Merge all data 
merged = lineitem.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged = merged.merge(supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged = merged.merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter data as in the query and group it
result = merged.loc[
    (merged.O_ORDERSTATUS == 'F') &
    (merged.flag_x == True) &
    (merged.flag_y != True) &
    (merged.N_NAME == 'SAUDI ARABIA')
].groupby('S_NAME').size().reset_index(name='NUMWAIT')

result.sort_values(['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)

# Write result to csv
result.to_csv('query_output.csv', index=False)
