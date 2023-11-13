# query.py

import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient

# Create connections to MySQL and MongoDB
mysql_engine = create_engine('mysql://root:my-secret-pw@mysql/tpch')
client = MongoClient('mongodb://mongodb:27017')

# Connect to tpch database
db = client['tpch']

# Query MySQL
mysql_query = """
SELECT
    N_NAME AS NATION,
    YEAR(O_ORDERDATE) AS O_YEAR,
    P_PARTKEY,
    PS_SUPPLYCOST,
    O_ORDERKEY,
    L_QUANTITY
FROM
    part,
    partsupp,
    orders,
    nation
WHERE
    PS_PARTKEY = P_PARTKEY
    AND O_ORDERKEY = L_ORDERKEY
    AND P_NAME LIKE '%dim%'
"""
mysql_df = pd.read_sql_query(mysql_query, mysql_engine)

# Query MongoDB
supplier = pd.DataFrame(list(db.supplier.find({}, {'_id': 0})))
lineitem = pd.DataFrame(list(db.lineitem.find({}, {'_id': 0})))

# Merge MySQL and Mongo DataFrames
merged_df = mysql_df.merge(supplier, left_on='PS_PARTKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(lineitem, left_on=['O_ORDERKEY', 'P_PARTKEY'], right_on=['L_ORDERKEY', 'L_PARTKEY'])

# Compute amount
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']

# Summarize profit
profit = merged_df.groupby(['NATION', 'O_YEAR']).AMOUNT.sum().reset_index()

# Sort result
profit = profit.sort_values(['NATION', 'O_YEAR'], ascending=[True, False])

# Save to CSV
profit.to_csv('query_output.csv', index=False)
