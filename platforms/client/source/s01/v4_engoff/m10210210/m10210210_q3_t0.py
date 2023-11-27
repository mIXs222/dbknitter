# query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Establish connections to the databases
# MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MongoDB Connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Redis Connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# MySQL Query to fetch data from lineitem table
mysql_query = """
SELECT
    L_ORDERKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT
FROM
    lineitem
WHERE
    L_SHIPDATE > '1995-03-15'
"""
lineitem_df = pd.read_sql(mysql_query, mysql_conn)

# Calculate revenue and filter orders with non-shipping status
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Fetch data from MongoDB orders collection
orders_coll = mongodb_db['orders']
orders_df = pd.DataFrame(list(orders_coll.find()))

# Filter orders for BUILDING market segment - handled in Python since it's in Redis
cust_keys_building_segment = [key.decode("utf-8").split(":")[1] for key in redis_conn.keys('customer:*') if redis_conn.hget(key, 'C_MKTSEGMENT').decode("utf-8") == 'BUILDING']

orders_df = orders_df[orders_df['O_CUSTKEY'].isin(cust_keys_building_segment)]
orders_df = orders_df[['O_ORDERKEY', 'O_SHIPPRIORITY']]

# Merge dataframes on O_ORDERKEY = L_ORDERKEY
result_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Get orders with largest revenue
result_df = result_df.nlargest(1, 'REVENUE')

# Selecting needed columns and sorting
output_df = result_df[['O_ORDERKEY', 'REVENUE', 'O_SHIPPRIORITY']].sort_values('REVENUE', ascending=False)

# Write to CSV
output_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongodb_client.close()
