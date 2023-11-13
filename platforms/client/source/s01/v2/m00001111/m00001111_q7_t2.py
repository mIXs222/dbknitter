import pandas as pd
from sqlalchemy import create_engine
import pymongo

# Create a connection to the MySQL database
mysql_conn = create_engine('mysql+mysqlconnector://root:my-secret-pw@mysql/tpch')
# Pull data from MySQL
nation_df = pd.read_sql('SELECT * FROM nation', mysql_conn)
supplier_df = pd.read_sql('SELECT * FROM supplier', mysql_conn)

# Create a connection to the MongoDB database
mongo_conn = pymongo.MongoClient("mongodb://mongodb:27017/")
# Access database
mongodb = mongo_conn["tpch"]
# Access collections
orders = mongodb["orders"]
customer = mongodb["customer"]
lineitem = mongodb["lineitem"]
# Fetch data and convert to pandas DataFrame
orders_df = pd.DataFrame(list(orders.find()))
customer_df = pd.DataFrame(list(customer.find()))
lineitem_df = pd.DataFrame(list(lineitem.find()))

# Renaming columns for convenience
nation_df.rename(columns={'N_NATIONKEY': 'NATIONKEY', 'N_NAME': 'NAME'}, inplace=True)
supplier_df.rename(columns={'S_SUPPKEY': 'SUPPKEY'}, inplace=True)

# Merge Datasets
merged_df = pd.merge(nation_df, supplier_df, left_on='NATIONKEY', right_on='SUPPKEY')
merged_df = pd.merge(merged_df, lineitem_df, left_on='SUPPKEY', right_on='L_SUPPKEY')
merged_df = pd.merge(merged_df, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = pd.merge(merged_df, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Filter
merged_df = merged_df[
    (
        ((merged_df.NAME_x == 'JAPAN') & (merged_df.NAME_y == 'INDIA'))
        | ((merged_df.NAME_x == 'INDIA') & (merged_df.NAME_y == 'JAPAN'))
    )
    & (merged_df.L_SHIPDATE >= '1995-01-01')
    & (merged_df.L_SHIPDATE <= '1996-12-31')
]

# Create 'L_YEAR' column
merged_df['L_YEAR'] = pd.to_datetime(merged_df['L_SHIPDATE']).dt.year

# Create 'VOLUME' column
merged_df['VOLUME'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Rename columns
merged_df.rename(columns={'NAME_x': 'SUPP_NATION', 'NAME_y': 'CUST_NATION'}, inplace=True)

# Aggregate
final_df = merged_df.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR']).agg(
    {"VOLUME": ["sum"]}).reset_index()
final_df.columns = ['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'REVENUE']

# Sorting
final_df = final_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write result to CSV
final_df.to_csv('query_output.csv', index=False)
