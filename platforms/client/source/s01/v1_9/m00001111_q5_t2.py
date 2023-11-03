import pandas as pd
from pymongo import MongoClient
import mysql.connector

# Connect to MySQL
config = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch',
    'raise_on_warnings': True
}
cnx = mysql.connector.connect(**config)

# Read data from MySQL
nation = pd.read_sql('SELECT * FROM NATION', con=cnx)
region = pd.read_sql('SELECT * FROM REGION', con=cnx)
part = pd.read_sql('SELECT * FROM PART', con=cnx)
supplier = pd.read_sql('SELECT * FROM SUPPLIER', con=cnx)

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['tpch']

# Read data from MongoDB
partsupp = pd.DataFrame(list(db.partsupp.find()))
customer = pd.DataFrame(list(db.customer.find()))
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# Merge dataframes
merged_df = pd.merge(customer, orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, supplier, left_on=['L_SUPPKEY', 'C_NATIONKEY'], right_on=['S_SUPPKEY', 'S_NATIONKEY'])
merged_df = pd.merge(merged_df, nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = pd.merge(merged_df, region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Apply conditions
filtered_df = merged_df[(merged_df['R_NAME'] == 'ASIA') & (merged_df['O_ORDERDATE'] >= '1990-01-01') & (merged_df['O_ORDERDATE'] < '1995-01-01')]

# Apply operations
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
result = filtered_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Sort
result = result.sort_values(by='REVENUE', ascending=False)

# Write to CSV
result.to_csv('query_output.csv', index=False)
