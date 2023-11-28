# query.py
import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM supplier")
    supplier_data = cursor.fetchall()

# Convert to DataFrame
supplier_columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
df_supplier = pd.DataFrame(supplier_data, columns=supplier_columns)

# Filter out suppliers with comments containing 'Customer Complaints'
df_supplier = df_supplier[~df_supplier['S_COMMENT'].str.contains('Customer Complaints')]

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_table = mongo_db['part']

# Get all parts and convert to DataFrame
df_part = pd.DataFrame(list(part_table.find()))

# Apply the filters to parts
df_part = df_part[
    (~df_part['P_BRAND'].str.contains('Brand#45')) &
    (~df_part['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (df_part['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
partsupp_data = redis_conn.get('partsupp')

# Convert to DataFrame
df_partsupp = pd.read_json(partsupp_data.decode("utf-8"))

# Perform the join operation
result = pd.merge(df_part, df_partsupp, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Group by brand, type, and size
result_grouped = result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])

# Calculate count of distinct suppliers
result_final = result_grouped.agg(SUPPLIER_CNT=pd.NamedAgg(column='S_SUPPKEY', aggfunc='nunique')).reset_index()

# Order the results
result_final = result_final.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write results to CSV
result_final.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()
