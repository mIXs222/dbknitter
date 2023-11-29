import pandas as pd
import pymysql
import pymongo
import direct_redis
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)
mysql_cursor = mysql_conn.cursor()

# Get customer data from MySQL
mysql_cursor.execute("SELECT C_CUSTKEY, C_NATIONKEY FROM customer WHERE C_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME IN ('INDIA', 'JAPAN'))")
customer_data = mysql_cursor.fetchall()

# Create DataFrame for customer data
customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NATIONKEY'])
customer_df['C_NATIONKEY'] = customer_df['C_NATIONKEY'].astype(str)

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Get lineitem data from MongoDB
lineitem_cursor = mongodb_db.lineitem.find({
    "L_SHIPDATE": {"$gte": datetime(1995, 1, 1), "$lte": datetime(1996, 12, 31)}
})
lineitem_df = pd.DataFrame(list(lineitem_cursor))

# Redis connection
redis_client = direct_redis.DirectRedis(port=6379, host="redis")

# Get nation data from Redis
nation_df = pd.read_json(redis_client.get('nation').decode('utf-8'))
nation_df['N_NATIONKEY'] = nation_df['N_NATIONKEY'].astype(str)

# Get supplier data from Redis
supplier_df = pd.read_json(redis_client.get('supplier').decode('utf-8'))
supplier_df['S_NATIONKEY'] = supplier_df['S_NATIONKEY'].astype(str)

# Join the data on keys and calculate the revenue
merged_df = lineitem_df.merge(customer_df, left_on='L_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df[(merged_df['C_NATIONKEY'] != merged_df['S_NATIONKEY'])]
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
merged_df['CUST_NATION'] = merged_df['C_NATIONKEY'].map(nation_df.set_index('N_NATIONKEY')['N_NAME'])
merged_df['SUPP_NATION'] = merged_df['S_NATIONKEY'].map(nation_df.set_index('N_NATIONKEY')['N_NAME'])
merged_df['L_YEAR'] = merged_df['L_SHIPDATE'].dt.year

# Filter rows for nations India and Japan
merged_df = merged_df[(merged_df['CUST_NATION'].isin(['INDIA', 'JAPAN'])) & (merged_df['SUPP_NATION'].isin(['INDIA', 'JAPAN']))]

# Group by the necessary fields and calculate sum of revenue
grouped_df = merged_df.groupby(['CUST_NATION', 'L_YEAR', 'SUPP_NATION'])
summed_df = grouped_df['REVENUE'].sum().reset_index()

# Order by supplier nation, customer nation, year - all ascending
ordered_df = summed_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Select and rename columns as per the instructions
output_df = ordered_df[['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']]

# Write to CSV
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mysql_conn.close()
mongodb_client.close()
redis_client.close()

print("Query output has been successfully written to 'query_output.csv'.")
