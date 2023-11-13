from pymongo import MongoClient
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# Connect to MySQL server
mysql_conn = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

# Connect to MongoDB server
mongo_conn = MongoClient('mongodb', 27017)

# Create a Pandas dataframe from each MySQL table
mysql_orders_df = pd.read_sql("SELECT * FROM orders WHERE O_ORDERDATE >= '1993-10-01' AND O_ORDERDATE < '1994-01-01'", mysql_conn)
mysql_nation_df = pd.read_sql("SELECT * FROM nation", mysql_conn)

# Create a Pandas dataframe from each MongoDB collection
mongo_customer_df = pd.DataFrame(list(mongo_conn["tpch"]["customer"].find()))
mongo_lineitem_df = pd.DataFrame(list(mongo_conn["tpch"]["lineitem"].find()))

# Merge the dataframes using conditions from the query
merged_df = pd.merge(mongo_customer_df, mysql_orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, mongo_lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, mysql_nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Filter data based on conditions from the query
filtered_df = merged_df.loc[merged_df['L_RETURNFLAG']=='R']

# Generate 'REVENUE' column and group the data according to the query
filtered_df['REVENUE'] = filtered_df.apply(lambda row: row.L_EXTENDEDPRICE * (1 - row.L_DISCOUNT), axis=1)
grouped_df = filtered_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'])['REVENUE'].sum().reset_index()

# Sort data according to the query
sorted_df = grouped_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Write output to CSV
sorted_df.to_csv('query_output.csv', index=False)

