# 1. Python Code

import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query for MySQL to retrieve lineitem data
mysql_cursor.execute("""
SELECT
    L_ORDERKEY,
    L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS revenue_volume,
    YEAR(L_SHIPDATE) AS ship_year
FROM lineitem
WHERE L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
""")
lineitem_data = mysql_cursor.fetchall()
lineitem_df = pd.DataFrame(lineitem_data, columns=['O_ORDERKEY', 'revenue_volume', 'ship_year'])

# Query for MongoDB to retrieve orders, supplier, nation data
orders_collection = mongo_db['orders']
supplier_collection = mongo_db['supplier']
nation_collection = mongo_db['nation']

supplier_df = pd.DataFrame(supplier_collection.find({}, {'_id': 0}))
nation_df = pd.DataFrame(nation_collection.find({}, {'_id': 0}))
orders_df = pd.DataFrame(orders_collection.find({}, {'_id': 0}))

# Retrieve customer data from Redis
customer_data = redis_client.get('customer')
customer_df = pd.read_json(customer_data)

# Combine the dataframes using merges and filters
merged_df = lineitem_df.merge(orders_df, how='left', left_on='O_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(customer_df, how='left', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(supplier_df, how='left', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation_df, how='left', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(nation_df, how='left', left_on='C_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_supplier', '_customer'))

# Filter for 'JAPAN' and 'INDIA' nations
merged_df = merged_df[(merged_df['N_NAME_supplier'].isin(['JAPAN', 'INDIA'])) & (merged_df['N_NAME_customer'].isin(['JAPAN', 'INDIA'])) & (merged_df['N_NAME_supplier'] != merged_df['N_NAME_customer'])]

# Group by and sum revenue
grouped_df = merged_df.groupby(['N_NAME_supplier', 'N_NAME_customer', 'ship_year']).agg({'revenue_volume': 'sum'}).reset_index()

# Sort the results
sorted_df = grouped_df.sort_values(by=['N_NAME_supplier', 'N_NAME_customer', 'ship_year'])

# Write to CSV
sorted_df.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
mongo_client.close()
redis_client.connection_pool.disconnect()
