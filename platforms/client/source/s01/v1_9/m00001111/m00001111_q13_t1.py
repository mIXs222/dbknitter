import pandas as pd
from pymongo import MongoClient
import pymysql

# configure MySQL connection
mysql_conn = pymysql.connect(host='mysql', 
                             user='root', 
                             password='my-secret-pw', 
                             db='tpch')

# configure MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# get data from MySQL tables
df_customer = pd.read_sql('SELECT * FROM customer', mysql_conn)
df_orders = pd.read_sql('SELECT * FROM orders', mysql_conn)

# get data from MongoDB tables
df_orders_mongo = pd.DataFrame(list(mongo_db['orders'].find()))
df_customer_mongo = pd.DataFrame(list(mongo_db['customer'].find()))

# concat data from MySQL and MongoDB
df_orders = pd.concat([df_orders, df_orders_mongo])
df_customer = pd.concat([df_customer, df_customer_mongo])

# execute the query
query_result = df_customer.merge(df_orders, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
query_result = query_result[~query_result['O_COMMENT'].str.contains('pending|deposits', na=False)]
query_result = query_result.groupby('C_CUSTKEY')['O_ORDERKEY'].nunique().reset_index(name='C_COUNT')
query_result = query_result.groupby('C_COUNT').size().reset_index(name='CUSTDIST')
query_result = query_result.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# write the result to csv
query_result.to_csv('query_output.csv', index=False)
