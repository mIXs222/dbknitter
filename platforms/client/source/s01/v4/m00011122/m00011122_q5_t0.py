# python code to execute the query on multiple databases and combine the results
import pymysql
import pymongo
import pandas as pd
import direct_redis

# MySQL connection and query execution
mysql_conn = pymysql.connect(
    host='mysql', user='root', password='my-secret-pw', db='tpch'
)
mysql_query = """
SELECT N_NATIONKEY, N_NAME, N_REGIONKEY
FROM nation
WHERE EXISTS (
    SELECT R_REGIONKEY FROM region WHERE N_REGIONKEY = R_REGIONKEY AND R_NAME = 'ASIA'
)
"""
nation_region_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# MongoDB connection and query execution
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']
customer_collection = mongo_db['customer']

supplier_df = pd.DataFrame(list(supplier_collection.find()))
customer_df = pd.DataFrame(list(customer_collection.find()))

# Filtering suppliers based on nation using previously fetched nation_region_df
suppliers_filtered = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_region_df['N_NATIONKEY'])]

# Filtering customers based on nation
customers_filtered = customer_df[customer_df['C_NATIONKEY'].isin(nation_region_df['N_NATIONKEY'])]

# Redis connection and data fetching
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))
redis_conn.close()

# Convert 'O_ORDERDATE' to datetime to filter orders for the provided date range
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_filtered = orders_df[
    (orders_df['O_ORDERDATE'] >= '1990-01-01') & (orders_df['O_ORDERDATE'] < '1995-01-01')
]

# Merging filtered dataframes on keys to get relevant data for processing
merged_df = customers_filtered.merge(orders_filtered, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = merged_df.merge(suppliers_filtered, left_on='C_NATIONKEY', right_on='S_NATIONKEY')

# Compute REVENUE and group by N_NAME
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
result = merged_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Join the result with nation_region_df to get the final result
final_result = nation_region_df.merge(result, on='N_NAME')

# Sort results by REVENUE in descending order
final_result = final_result.sort_values('REVENUE', ascending=False)

# Write output to CSV
final_result.to_csv('query_output.csv', index=False)
