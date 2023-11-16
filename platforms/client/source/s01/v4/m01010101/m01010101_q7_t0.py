# query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']
customer_collection = mongo_db['customer']
lineitem_collection = mongo_db['lineitem']

# Convert MongoDB results to a list of dictionaries
supplier = list(supplier_collection.find({}, {'_id':0}))
customer = list(customer_collection.find({}, {'_id':0}))
lineitem = list(lineitem_collection.find({}, {'_id':0}))

# Use pandas to facilitate data manipulation
import pandas as pd

# Convert the supplier, customer, and lineitem collections into pandas DataFrames
supplier_df = pd.DataFrame(supplier)
customer_df = pd.DataFrame(customer)
lineitem_df = pd.DataFrame(lineitem)

with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM nation;")
    nations = cursor.fetchall()
    nation_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

# Merge the supplier and nation data
supplier_nation = pd.merge(supplier_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
supplier_nation.rename(columns={'N_NAME': 'SUPP_NATION'}, inplace=True)

# Merge the customer and nation data
customer_nation = pd.merge(customer_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
customer_nation.rename(columns={'N_NAME': 'CUST_NATION'}, inplace=True)

# Execute orders query on MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM orders;")
    orders = cursor.fetchall()
    orders_df = pd.DataFrame(orders, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 
                                              'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 
                                              'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

# Combine all needed data into a single DataFrame
merged_df = (
    lineitem_df
    .merge(supplier_nation, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customer_nation, left_on='L_ORDERKEY', right_on='C_CUSTKEY')
)

# Filter for the specified nations and dates
filtered_df = merged_df[
    (
        ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['CUST_NATION'] == 'INDIA')) |
        ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['CUST_NATION'] == 'JAPAN'))
    ) &
    (merged_df['L_SHIPDATE'] >= '1995-01-01') &
    (merged_df['L_SHIPDATE'] <= '1996-12-31')
]

# Add necessary columns
filtered_df['L_YEAR'] = pd.to_datetime(filtered_df['L_SHIPDATE']).dt.year
filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by the specified columns
result_df = (
    filtered_df
    .groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])
    .agg({'VOLUME': 'sum'})
    .reset_index()
)

# Sort the results
result_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Write results to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
