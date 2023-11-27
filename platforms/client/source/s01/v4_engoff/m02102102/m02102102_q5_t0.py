# query.py

import pymysql
import pymongo
import pandas as pd
import direct_redis

# Create a connection to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Create a connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Create a Redis client using DirectRedis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Querying MySQL for nations and suppliers in ASIA region
with mysql_connection.cursor() as cursor:
    cursor.execute("""
        SELECT n.N_NATIONKEY, n.N_NAME
        FROM nation n
        JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
        WHERE r.R_NAME = 'ASIA'
    """)
    nations = cursor.fetchall()

    cursor.execute("""
        SELECT s.S_SUPPKEY, s.S_NATIONKEY
        FROM supplier s
        JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
        JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
        WHERE r.R_NAME = 'ASIA'
    """)
    suppliers = cursor.fetchall()

# Querying MongoDB for customers in ASIA
customers = list(mongo_db.customer.find({'C_NATIONKEY': {'$in': [n[0] for n in nations]}}))

# Convert customers to Pandas DataFrame
customer_df = pd.DataFrame(customers)

# Querying Redis for lineitems and regions
df_lineitem = pd.read_json(redis_client.get('lineitem').decode('utf-8'))
df_region = pd.read_json(redis_client.get('region').decode('utf-8'))

# Filter lineitems within the date range and join with the suppliers
df_lineitem_filtered = df_lineitem[
    (df_lineitem['L_SHIPDATE'] >= '1990-01-01') &
    (df_lineitem['L_SHIPDATE'] <= '1995-01-01')
]

# Compute the revenue
df_lineitem_filtered['REVENUE'] = df_lineitem_filtered['L_EXTENDEDPRICE'] * (1 - df_lineitem_filtered['L_DISCOUNT'])

# Join to get the customers and suppliers from the same nation
local_supplier_volume_query = pd.merge(
    df_lineitem_filtered,
    customer_df,
    how="inner",
    left_on="L_ORDERKEY",
    right_on="C_CUSTKEY"
)

# Further join with suppliers that are in the same nation
local_supplier_volume_query = pd.merge(
    local_supplier_volume_query,
    pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NATIONKEY']),
    how="inner", 
    left_on=["L_SUPPKEY", "C_NATIONKEY"],
    right_on=["S_SUPPKEY", "S_NATIONKEY"]
)

# Group by nation and calculate the sum of revenue
result_df = local_supplier_volume_query.groupby(['C_NATIONKEY'])['REVENUE'].sum().reset_index()

# Rename columns as per the requirements
result_df.columns = ['NATION', 'REVENUE']

# Sort the results by revenue in descending order
result_df = result_df.sort_values(by='REVENUE', ascending=False)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_connection.close()
mongo_client.close()
