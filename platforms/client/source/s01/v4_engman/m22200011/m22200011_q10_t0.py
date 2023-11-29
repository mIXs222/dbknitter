# file: query_code.py

import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Select customers from MySQL
mysql_query = """
SELECT C_CUSTKEY, C_NAME, C_ACCTBAL, C_ADDRESS, C_PHONE, C_COMMENT
FROM customer;
"""

customers_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Select orders and lineitem from MongoDB
orders_coll = mongo_db['orders']
lineitem_coll = mongo_db['lineitem']

orders_df = pd.DataFrame(list(orders_coll.find(
    {"O_ORDERDATE": {"$gte": "1993-10-01", "$lt": "1994-01-01"}}
)))

lineitem_df = pd.DataFrame(list(lineitem_coll.find()))

# Merging orders with lineitem
merged_df = pd.merge(orders_df, lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate revenue lost
merged_df['REVENUE_LOST'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Sum revenue lost grouped by O_CUSTKEY
revenue_lost_df = merged_df.groupby('O_CUSTKEY').agg({'REVENUE_LOST': 'sum'}).reset_index()

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load nation DataFrame from Redis
nation_df = pd.read_json(r.get('nation').decode('utf-8'))

# Merge customers with revenue lost and nation
final_df = pd.merge(customers_df, revenue_lost_df, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
final_df = pd.merge(final_df, nation_df, how='left', left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Selecting required columns
final_df = final_df[['C_CUSTKEY', 'C_NAME', 'REVENUE_LOST', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]

# Sort by revenue lost in ascending, customer key and name in ascending, and account balance in descending
final_df.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], 
                     ascending=[True, True, True, False], inplace=True)

# Output to CSV
final_df.to_csv('query_output.csv', index=False)
