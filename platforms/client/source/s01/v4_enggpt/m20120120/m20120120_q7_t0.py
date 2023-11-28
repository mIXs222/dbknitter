import pandas as pd
import pymysql
import pymongo
from direct_redis import DirectRedis

# MySQL connection
mysql_db = pymysql.connect(user='root', password='my-secret-pw', database='tpch', host='mysql')
cursor = mysql_db.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
customer_collection = mongodb['customer']

# Redis connection
redis = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for lineitem and nation tables
query_lineitem = """
SELECT
  L_ORDERKEY,
  L_EXTENDEDPRICE,
  L_DISCOUNT,
  L_SHIPDATE,
  S_NATIONKEY AS SUPPLIER_NATION,
  O_CUSTKEY
FROM
  lineitem
JOIN
  orders ON lineitem.L_ORDERKEY = orders.O_ORDERKEY
WHERE
  L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""
cursor.execute(query_lineitem)
lineitems = cursor.fetchall()

# Load data into pandas dataframe
lineitems_df = pd.DataFrame(lineitems, columns=['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPDATE', 'SUPPLIER_NATION', 'O_CUSTKEY'])

# Get customer table from MongoDB
customers_df = pd.DataFrame(list(customer_collection.find({}, {'_id': 0, 'C_CUSTKEY': 1, 'C_NATIONKEY': 1})))

# Get nation table from Redis
nation_df = pd.read_json(redis.get('nation'), orient='records')

# Merge dataframes
merged_df = pd.merge(lineitems_df, customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')
merged_df = pd.merge(merged_df, nation_df, left_on='SUPPLIER_NATION', right_on='N_NATIONKEY', how='inner')

# Filter for Indian and Japanese nations
filtered_df = merged_df.loc[(merged_df['N_NAME'] == 'INDIA') | (merged_df['N_NAME'] == 'JAPAN')]

# Calculate revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Extract year from SHIPDATE
filtered_df['YEAR'] = pd.to_datetime(filtered_df['L_SHIPDATE']).dt.year

# Group by necessary fields
grouped_df = filtered_df.groupby(['N_NAME', 'YEAR']).agg({'REVENUE': 'sum'}).reset_index()

# Sort by specified columns
grouped_df.sort_values(['N_NAME', 'YEAR'], ascending=[True, True], inplace=True)

# Write output to CSV
grouped_df.to_csv('query_output.csv', index=False)

# Close the database connections
cursor.close()
mysql_db.close()
mongo_client.close()
redis.close()
