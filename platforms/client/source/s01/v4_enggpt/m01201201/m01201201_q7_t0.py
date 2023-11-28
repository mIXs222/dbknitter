import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
# Query for MySQL
mysql_query = """SELECT s.S_NAME as SUPPLIER_NAME, n.N_NAME as NATION_NAME, s.S_NATIONKEY, o.O_ORDERKEY
                 FROM supplier as s
                 JOIN nation as n ON s.S_NATIONKEY = n.N_NATIONKEY
                 JOIN orders as o ON s.S_NATIONKEY = o.O_CUSTKEY
                 WHERE n.N_NAME IN ('JAPAN', 'INDIA')"""
supplier_orders_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']
# MongoDB query
lineitem_query = {
    "L_SHIPDATE": {"$gte": datetime(1995, 1, 1), "$lte": datetime(1996, 12, 31)}
}
lineitem_cursor = lineitem_collection.find(lineitem_query)
lineitem_df = pd.DataFrame(list(lineitem_cursor))
mongo_client.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
# Redis query
customer_json = redis_client.get('customer')
customer_df = pd.read_json(customer_json, orient='split')

# Filter nations and merge dataframes
supplier_nation_df = supplier_orders_df[supplier_orders_df['NATION_NAME'].isin(['JAPAN', 'INDIA'])]
customer_nation_df = customer_df[customer_df['C_NATIONKEY'].isin(supplier_nation_df['S_NATIONKEY'])]

# Merge with lineitem dataframe and calculate revenue
merged_df = pd.merge(supplier_nation_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, customer_nation_df, left_on='O_ORDERKEY', right_on='C_CUSTKEY')
merged_df['YEAR'] = merged_df['L_SHIPDATE'].dt.year
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group by and sort
result_df = merged_df.groupby(['SUPPLIER_NAME', 'NATION_NAME', 'YEAR'], as_index=False)['REVENUE'].sum()
result_df.sort_values(by=['SUPPLIER_NAME', 'NATION_NAME', 'YEAR'], inplace=True)

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
