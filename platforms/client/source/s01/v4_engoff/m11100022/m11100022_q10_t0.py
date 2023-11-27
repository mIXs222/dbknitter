# import necessary libraries
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Establish a connection to the MySQL database and fetch customers data
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
customer_query = "SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_PHONE, C_ACCTBAL, C_COMMENT FROM customer;"
mysql_df = pd.read_sql(customer_query, mysql_conn)
mysql_conn.close()

# Establish a connection to the MongoDB database and fetch nations data
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation_collection = mongo_db["nation"]
nation_df = pd.DataFrame(list(nation_collection.find()))

# Establish a connection with the Redis database and fetch lineitems and orders data
redis_client = DirectRedis(host='redis', port=6379, db=0)
orders_df = pd.read_json(redis_client.get('orders'))
lineitems_df = pd.read_json(redis_client.get('lineitem'))

# Filter orders and lineitems data for the specified quarter
start_date = datetime(1993, 10, 1)
end_date = datetime(1994, 1, 1)
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
filtered_orders = orders_df[(orders_df['O_ORDERDATE'] >= start_date) & (orders_df['O_ORDERDATE'] <= end_date)]

# Merge tables and calculate lost revenue
merged_lineitems = pd.merge(filtered_orders, lineitems_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_lineitems["LOST_REVENUE"] = merged_lineitems["L_EXTENDEDPRICE"] * (1 - merged_lineitems["L_DISCOUNT"])

customers_revenue = merged_lineitems.groupby('O_CUSTKEY')["LOST_REVENUE"].sum().reset_index()
merged_customers = pd.merge(mysql_df, customers_revenue, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_results = pd.merge(merged_customers, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select required columns ans sort the result
final_result = merged_results[[
    "C_NAME", "C_ADDRESS", "N_NAME", "C_PHONE", "C_ACCTBAL", "C_COMMENT", "LOST_REVENUE"
]]
final_result.rename(columns={'N_NAME': 'NATION'}, inplace=True)
final_result.sort_values(by=['LOST_REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True], inplace=True)

# Write the result to a CSV file
final_result.to_csv("query_output.csv", index=False)
