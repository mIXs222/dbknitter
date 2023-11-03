import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
import csv

# Connect to MySQL
engine = create_engine('mysql+mysqlconnector://root:my-secret-pw@mysql/tpch')

# Execute MySQL portion of the query
mysql_query = """
SELECT
    PS_PARTKEY,
    PS_SUPPKEY,
    PS_SUPPLYCOST,
    N_NAME,
    N_NATIONKEY,
    P_PARTKEY,
    P_NAME
FROM
    part,
    partsupp,
    nation
WHERE
    PS_PARTKEY = P_PARTKEY
    AND S_NATIONKEY = N_NATIONKEY
    AND P_NAME LIKE '%dim%'
"""
df_mysql = pd.read_sql_query(mysql_query, engine)

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client.tpch

# MongoDB queries and data gathering
supplier = mongodb.suppiler.find({}, {"_id": 0})
orders = mongodb.orders.find({}, {"_id": 0})
lineitem = mongodb.lineitem.find({}, {"_id": 0})

# Converting MongoDB data to pandas DataFrame
df_supplier = pd.DataFrame(list(supplier))
df_orders = pd.DataFrame(list(orders))
df_lineitem = pd.DataFrame(lineitem)

# Merge DataFrames
merged_data = (df_lineitem.merge(df_orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
                            .merge(df_supplier, how='inner', left_on='S_SUPPKEY', right_on='S_SUPPKEY')
                            .merge(df_mysql, how='inner', left_on=['L_PARTKEY', 'S_SUPPKEY'], right_on=['P_PARTKEY', 'PS_SUPPKEY']))

# Additional data computation
merged_data['O_YEAR'] = pd.to_datetime(merged_data['O_ORDERDATE']).dt.year
merged_data['AMOUNT'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT']) - merged_data['PS_SUPPLYCOST'] * merged_data['L_QUANTITY']

# Grouping and sorting
output = (merged_data.groupby(['N_NAME', 'O_YEAR']).AMOUNT.sum().reset_index()
                                               .sort_values(['N_NAME', 'O_YEAR'], ascending=[True, False]))

# Save to CSV
output.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
