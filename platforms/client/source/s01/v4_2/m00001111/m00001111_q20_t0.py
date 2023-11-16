import pymysql.cursors
from pymongo import MongoClient
import pandas as pd
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)
mysql_query = """
SELECT
    S_SUPPKEY,
    S_NAME,
    S_ADDRESS,
    S_NATIONKEY
FROM
    supplier,
    nation
WHERE 
    S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'CANADA'
"""
mysql_df = pd.read_sql(mysql_query, mysql_conn)

# MongoDB connection
mongodb_conn = MongoClient('mongodb', 27017)
mongodb_db = mongodb_conn['tpch']

partsupp = list(mongodb_db.partsupp.find())
lineitem = list(mongodb_db.lineitem.find())

# Transform MongoDB data to pandas DataFrame
partsupp_df = pd.DataFrame(partsupp)
lineitem_df = pd.DataFrame(lineitem)

# Filtering on partsupp using the condition on part from the SQL query
partsupp_df = partsupp_df[partsupp_df['PS_PARTKEY'].str.startswith('forest')]

# Aggregation on lineitem
agg_lineitem = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY']).agg({'L_QUANTITY': 'sum'}).reset_index()

# Subquery join and filtering
final_df = partsupp_df.merge(agg_lineitem, left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
final_df = final_df[final_df['PS_AVAILQTY'] > final_df['L_QUANTITY']*0.5]

# Ignore suppliers not in MySQL result
final_df = final_df[final_df['PS_SUPPKEY'].isin(mysql_df['S_SUPPKEY'])]

# Merge MySQL and MongoDB's DataFrames
final_df = final_df.merge(mysql_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Sorting
final_df = final_df.sort_values('S_NAME')

# Write the result to a .csv file
final_df.to_csv('query_output.csv', quoting=csv.QUOTE_NONNUMERIC, index=False)
