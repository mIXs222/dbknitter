import csv
import pandas as pd
import mysql.connector
from pymongo import MongoClient

# Connect to MySQL and execute SELECT query to fetch data from part table
mysql_db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
cursor_mysql = mysql_db.cursor()

cursor_mysql.execute("""
SELECT *
FROM part
WHERE P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
""")
part_rows = cursor_mysql.fetchall()
part_df = pd.DataFrame(part_rows, columns=[i[0] for i in cursor_mysql.description])

cursor_mysql.execute("""
SELECT *
FROM supplier
WHERE S_COMMENT LIKE '%Customer%Complaints%'
""")
supplier_rows = cursor_mysql.fetchall()
supplier_df = pd.DataFrame(supplier_rows, columns=[i[0] for i in cursor_mysql.description])
mysql_db.close()


# Connect to MongoDB and fetch data from partsupp table
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client.tpch
partsupp_data = mongo_db.partsupp.find()
partsupp_df = pd.DataFrame(list(partsupp_data))
mongo_client.close()


# Merge the data from MySQL and MongoDB dataframes
merged_df = pd.merge(part_df, partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY', how='inner')

# Filter rows which are NOT IN supplier_df
merged_df = merged_df[~merged_df['PS_SUPPKEY'].isin(supplier_df['S_SUPPKEY'])]

# Group by and sort as per the query
result_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].nunique()
result_df = result_df.reset_index()
result_df = result_df.sort_values(['PS_SUPPKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])
result_df.rename(columns={'PS_SUPPKEY': 'SUPPLIER_CNT'}, inplace=True)

# Write output to CSV file
result_df.to_csv('query_output.csv', index=False)
