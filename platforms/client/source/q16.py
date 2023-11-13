from pymongo import MongoClient
import mysql.connector
import pandas as pd

# Connect to MongoDB for 'part' and 'partsupp' tables
mongo_client = MongoClient("mongodb://mongodb:27017")
mongo_db = mongo_client["tpch"]
part_col = mongo_db["part"]
partsupp_col = mongo_db["partsupp"]

# Connect to MySQL for 'supplier' table
mysql_conn = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mysql_cursor = mysql_conn.cursor(buffered=True)

# First, get suppliers with 'customer complaints'
mysql_cursor.execute("SELECT S_SUPPKEY FROM SUPPLIER WHERE S_COMMENT LIKE '%Customer%Complaints%'")
blocked_suppliers = [item[0] for item in mysql_cursor]

# Then, get parts and partsupp data from MongoDB
part_data = part_col.find({'P_BRAND': {'$ne': 'Brand#45'}, 
                           'P_TYPE': {'$not': {'$regex': 'MEDIUM POLISHED'}}, 
                           'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}})
partsupp_data = partsupp_col.find({'PS_SUPPKEY': {'$nin': blocked_suppliers}})

part_df = pd.DataFrame(list(part_data))
partsupp_df = pd.DataFrame(list(partsupp_data))

# Merge two dataframes and do calculation
result_df = partsupp_df.merge(part_df, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')
result_df = result_df.groupby(['P_BRAND','P_TYPE','P_SIZE'])['PS_SUPPKEY'].nunique().reset_index()
result_df.columns = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT']

# Sort dataframe
result_df = result_df.sort_values(['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Save dataframe to a CSV file
result_df.to_csv('output.csv', index=False)