import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
partsupp_collection = mongo_db['partsupp']

# Query parts and suppliers from MySQL
parts_query = """
    SELECT
        P_PARTKEY,
        P_NAME,
        P_MFGR,
        P_BRAND,
        P_TYPE,
        P_SIZE,
        P_RETAILPRICE,
        P_COMMENT
    FROM part
    WHERE P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
"""

suppliers_query = """
    SELECT
        S_SUPPKEY,
        S_NAME
    FROM supplier
    WHERE S_COMMENT NOT LIKE '%%Customer Complaints%%'
"""

parts_df = pd.read_sql(parts_query, mysql_conn)
suppliers_df = pd.read_sql(suppliers_query, mysql_conn)

# Query partsupp from MongoDB
partsupp_documents = partsupp_collection.find({})

# Convert MongoDB documents to DataFrame
partsupp_df = pd.DataFrame(partsupp_documents)

# Merge DataFrames
merged_df = pd.merge(
    parts_df,
    partsupp_df,
    left_on='P_PARTKEY',
    right_on='PS_PARTKEY',
    how='inner'
)

filtered_df = pd.merge(
    merged_df,
    suppliers_df,
    left_on='PS_SUPPKEY',
    right_on='S_SUPPKEY',
    how='inner'
)

# Group by specified columns and count distinct suppliers
grouped_df = filtered_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=pd.NamedAgg(column='S_SUPPKEY', aggfunc='nunique')).reset_index()

# Sort the results
sorted_df = grouped_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Output to CSV
sorted_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongo_client.close()
