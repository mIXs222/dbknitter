# query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Define connection information
mysql_conn_info = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch"
}
mongodb_conn_info = {
    "host": "mongodb",
    "port": 27017,
    "db": "tpch",
}
redis_conn_info = {
    "host": "redis",
    "port": 6379,
    "db": 0,
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient(**mongodb_conn_info)
mongodb = mongodb_client[mongodb_conn_info["db"]]

# Connect to Redis
redis_conn = DirectRedis(host=redis_conn_info["host"], port=redis_conn_info["port"], db=redis_conn_info["db"])

# Fetch data from MySQL and MongoDB
with mysql_conn.cursor() as cursor:
    cursor.execute("""
    SELECT C_CUSTKEY, C_NAME, C_ACCTBAL, C_ADDRESS, C_PHONE, C_COMMENT, C_NATIONKEY
    FROM customer
    """)
    customers = pd.DataFrame(cursor.fetchall(), columns=["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_ADDRESS", "C_PHONE", "C_COMMENT", "C_NATIONKEY"])

# Fetch data from MongoDB
nations = pd.DataFrame(list(mongodb.nation.find({}, {"_id": 0})))

# Fetch data from Redis
orders_data = pd.read_json(redis_conn.get('orders'), orient='records')
lineitem_data = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Filter for the given date range
start_date = datetime(1993, 10, 1)
end_date = datetime(1994, 1, 1)
filtered_orders = orders_data[(orders_data['O_ORDERDATE'] >= start_date) & (orders_data['O_ORDERDATE'] <= end_date)]

# Calculate revenue lost
lineitem_data['REVENUE_LOST'] = lineitem_data['L_EXTENDEDPRICE'] * (1 - lineitem_data['L_DISCOUNT'])

# Merge the data based on customer and nation key
merged_data = pd.merge(filtered_orders, lineitem_data, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_data = pd.merge(merged_data, customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_data = pd.merge(merged_data, nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Group by customer and calculate revenue loss per customer
grouped_data = merged_data.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT', 'N_NAME']).agg({'REVENUE_LOST': 'sum'})

# Sort results based on the given criteria
results = grouped_data.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False]).reset_index()

# Write results to CSV
results.to_csv('query_output.csv', index=False)
