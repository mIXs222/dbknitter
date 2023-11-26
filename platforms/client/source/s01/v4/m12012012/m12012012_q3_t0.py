# query.py

import pandas as pd
import pymysql
import pymongo
from direct_redis import DirectRedis
from bson.codec_options import CodecOptions
from datetime import datetime

# MySQL Connection and Query Execution
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Query for MySQL Customer data with 'BUILDING' market segment
mysql_query = """
SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT
FROM customer
WHERE C_MKTSEGMENT = 'BUILDING'
"""

# Execute Query
mysql_cursor.execute(mysql_query)
customer_data = pd.DataFrame(mysql_cursor.fetchall(), columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'])

# MongoDB Connection and Query Execution
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
mongodb_orders = mongodb_db.get_collection('orders', codec_options=CodecOptions(tz_aware=True))

# Query for MongoDB Orders data with date condition
orders_data = pd.DataFrame(list(mongodb_orders.find({
    "O_ORDERDATE": {"$lt": datetime(1995, 3, 15)}
}, projection={"_id": False})))

# Redis Connection and Data Retrieval
redis_client = DirectRedis(host='redis', port=6379)

# Get Lineitem data from Redis
lineitem_data_str = redis_client.get('lineitem')
if lineitem_data_str:
    lineitem_data = pd.read_json(lineitem_data_str, orient='split')
else:
    lineitem_data = pd.DataFrame()

# Filtering the Lineitem data with date condition
lineitem_data = lineitem_data[lineitem_data['L_SHIPDATE'] > '1995-03-15']

# Merging dataframes
merged_df = customer_data.merge(orders_data, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(lineitem_data, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate REVENUE and Apply Grouping
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
result = merged_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])['REVENUE'].sum().reset_index()
result = result.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write result to CSV file
result.to_csv('query_output.csv', index=False)

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
redis_client.close()
