# query_code.py
import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    # Query for customer and supplier data from MySQL
    mysql_query = """
    SELECT
        c.C_CUSTKEY,
        c.C_NATIONKEY,
        s.S_SUPPKEY,
        s.S_NATIONKEY
    FROM customer AS c
    INNER JOIN supplier AS s ON c.C_NATIONKEY = s.S_NATIONKEY
    WHERE c.C_MKTSEGMENT = 'ASIA';
    """
    cursor.execute(mysql_query)
    customer_supplier_data = cursor.fetchall()

# Convert MySQL data to DataFrame
customer_supplier_df = pd.DataFrame(customer_supplier_data, columns=['C_CUSTKEY', 'C_NATIONKEY', 'S_SUPPKEY', 'S_NATIONKEY'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
# Query for nation and region data from MongoDB
nation_data = list(mongo_db['nation'].find({"N_NAME": "ASIA"}, {'_id': 0}))
region_data = list(mongo_db['region'].find({}, {'_id': 0}))

# Convert MongoDB data to DataFrame
nation_df = pd.DataFrame(nation_data)
region_df = pd.DataFrame(region_data)
# Merging nation and region data
nation_region_df = pd.merge(nation_df, region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
# Get orders and lineitem data from Redis
orders_data = redis_conn.get('orders')
lineitem_data = redis_conn.get('lineitem')

# Convert Redis data to DataFrame
orders_df = pd.read_json(orders_data)
lineitem_df = pd.read_json(lineitem_data)

# Format order date in orders dataframe
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter data to the required date range
date_start = datetime(1990, 1, 1)
date_end = datetime(1994, 12, 31)
filtered_orders_df = orders_df[(orders_df['O_ORDERDATE'] >= date_start) & (orders_df['O_ORDERDATE'] <= date_end)]

# Merge the dataframes based on their keys
merged_df = pd.merge(filtered_orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, customer_supplier_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_grouped_df = pd.merge(merged_df, nation_region_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate revenue and group by nation name
merged_grouped_df['REVENUE'] = merged_grouped_df['L_EXTENDEDPRICE'] * (1 - merged_grouped_df['L_DISCOUNT'])
result_df = merged_grouped_df.groupby(['N_NAME'])['REVENUE'].sum().reset_index()
result_df = result_df.sort_values(by='REVENUE', ascending=False)

# Write the final result to a CSV file
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()
