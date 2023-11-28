# query.py
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
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis using DirectRedis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
nation_query = "SELECT * FROM nation"
region_query = "SELECT * FROM region WHERE R_NAME = 'ASIA'"
mysql_cursor.execute(nation_query)
nations = mysql_cursor.fetchall()
mysql_cursor.execute(region_query)
regions = mysql_cursor.fetchall()

# Convert MySQL data to DataFrame
df_nations = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
df_regions = pd.DataFrame(regions, columns=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])

# Fetch data from MongoDB
suppliers = mongo_db.supplier.find()
customers = mongo_db.customer.find()

# Convert MongoDB data to DataFrame
df_suppliers = pd.DataFrame(list(suppliers))
df_customers = pd.DataFrame(list(customers))

# Fetch data from Redis and convert to DataFrame
orders = redis_client.get('orders')
lineitems = redis_client.get('lineitem')
df_orders = pd.read_json(orders)
df_lineitems = pd.read_json(lineitems)

# Filter the data
asia_nations = df_regions.merge(df_nations, left_on='R_REGIONKEY', right_on='N_REGIONKEY')
asia_customers = asia_nations.merge(df_customers, left_on='N_NATIONKEY', right_on='C_NATIONKEY')
filtered_orders = df_orders[(df_orders['O_ORDERDATE'] >= datetime(1990, 1, 1)) & (df_orders['O_ORDERDATE'] <= datetime(1994, 12, 31))]

# Merge dataframes to prepare for final computation
merged_df = asia_customers.merge(filtered_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(df_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = merged_df.merge(df_suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Compute the total revenue by nation
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
grouped_revenue = merged_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sort the results
sorted_revenue = grouped_revenue.sort_values(by='REVENUE', ascending=False)

# Write the output to a CSV file
sorted_revenue.to_csv('query_output.csv', index=False)

# Clean up connections
mysql_conn.close()
mongo_client.close()
