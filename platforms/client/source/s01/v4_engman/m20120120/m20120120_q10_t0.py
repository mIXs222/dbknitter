# query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
conn_mysql = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor_mysql = conn_mysql.cursor()

# Connect to MongoDB
client_mongodb = pymongo.MongoClient('mongodb', 27017)
db_mongodb = client_mongodb['tpch']

# Connect to Redis
client_redis = DirectRedis(host='redis', port=6379, db=0)

# Query to get lineitems from mysql
query_lineitem = """SELECT L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT FROM lineitem
                    WHERE L_RETURNFLAG = 'R'
                    AND L_SHIPDATE >= '1993-10-01'
                    AND L_SHIPDATE <= '1994-01-01';"""
cursor_mysql.execute(query_lineitem)
lineitems = pd.DataFrame(cursor_mysql.fetchall(), columns=['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])

# Get nation and orders from Redis
nation_df = pd.read_json(client_redis.get('nation'))
orders_df = pd.read_json(client_redis.get('orders'))

# Get customers from MongoDB
customers = db_mongodb['customer'].find({})
customers_df = pd.DataFrame(list(customers))

# Calculate lost revenue and perform merge operations
lineitems['REVENUE_LOST'] = lineitems['L_EXTENDEDPRICE'] * (1 - lineitems['L_DISCOUNT'])

orders_with_lost_revenue = pd.merge(orders_df, lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
customers_complete = pd.merge(customers_df, orders_with_lost_revenue, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result = pd.merge(customers_complete, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select and sort the final columns
columns_to_select = ['C_CUSTKEY', 'C_NAME', 'REVENUE_LOST', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']
final_result = result[columns_to_select].copy()
final_result.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False], inplace=True)

# Write the final DataFrame to CSV
final_result.to_csv('query_output.csv', index=False)

# Close MySQL connection
cursor_mysql.close()
conn_mysql.close()
