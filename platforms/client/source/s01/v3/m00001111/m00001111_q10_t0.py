import pymongo
from pymongo import MongoClient
import mysql.connector
import pandas as pd

# Configurations
mysql_config = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch',
    'raise_on_warnings': True
}

mongo_config = {
    'host': 'mongodb',
    'port': 27017,
    'database': 'tpch'
}

# Create connection to MySQL
mysql_db = mysql.connector.connect(**mysql_config)

# Fetch Nation data from MySQL
nation_query = 'SELECT * FROM nation'
nation_df = pd.read_sql(nation_query, con=mysql_db)

# Create MongoClient
client = MongoClient(**mongo_config)

# Select tpch database
mongo_db = client['tpch']

# Fetch Customer, Orders, and Lineitem data from MongoDB
customer_df = pd.DataFrame(list(mongo_db['customer'].find()))
orders_df = pd.DataFrame(list(mongo_db['orders'].find()))
lineitem_df = pd.DataFrame(list(mongo_db['lineitem'].find()))

# Merge all dataframes based on appropriate attributes
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Filter based on the conditions provided
filtered_df = merged_df[
    (merged_df['O_ORDERDATE'] >= '1993-10-01') &
    (merged_df['O_ORDERDATE'] < '1994-01-01') &
    (merged_df['L_RETURNFLAG'] == 'R')
]

# Add a new column REVENUE
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by required columns and calculate SUM of REVENUE
grouped_df = filtered_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']).agg({
    'REVENUE': 'sum'
}).reset_index()

# Order by REVENUE, C_CUSTKEY and C_NAME
grouped_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False], inplace=True)

# Output results to a CSV file
grouped_df.to_csv('query_output.csv', index=False)
