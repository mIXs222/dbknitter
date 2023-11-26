# dependencies.py
import pymysql
from pymongo import MongoClient
import pandas as pd
import direct_redis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

mysql_cursor.execute("SELECT L_PARTKEY, L_SUPPKEY, SUM(L_QUANTITY) FROM lineitem WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01' GROUP BY L_PARTKEY, L_SUPPKEY")
lineitem_data = mysql_cursor.fetchall()

# Convert to DataFrame for easier manipulation
lineitem_df = pd.DataFrame(lineitem_data, columns=['L_PARTKEY', 'L_SUPPKEY', 'SUM_L_QUANTITY'])

mysql_conn.close()

# MongoDB connection and query
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

part_docs = mongo_db['part'].find({'P_NAME': {'$regex': '^forest'}})
part_df = pd.DataFrame(list(part_docs))

nation_docs = mongo_db['nation'].find({'N_NAME': 'CANADA'})
nation_df = pd.DataFrame(list(nation_docs))

# Redis connection and query
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

supplier_data = redis_conn.get('supplier')
supplier_df = pd.read_json(supplier_data)

partsupp_data = redis_conn.get('partsupp')
partsupp_df = pd.read_json(partsupp_data)

# Merging and filtering information
# Filter partsupp by parts and calculate the quantity constraint
filtered_partsupp_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_df['P_PARTKEY'])]

# Merge with lineitem on PARTKEY and SUPPKEY and filter by the quantity constraint
parts_lineitems_merged = pd.merge(filtered_partsupp_df, lineitem_df, left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
valid_partsupp = parts_lineitems_merged[parts_lineitems_merged['PS_AVAILQTY'] > 0.5 * parts_lineitems_merged['SUM_L_QUANTITY']]

# Merge supplier with nation and filter by nation
supplier_nation_merged = pd.merge(supplier_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Final select statement
result_df = supplier_nation_merged[supplier_nation_merged['S_SUPPKEY'].isin(valid_partsupp['PS_SUPPKEY'])][['S_NAME', 'S_ADDRESS']]

# Sort the result
result_df_sorted = result_df.sort_values('S_NAME')

# Write to CSV file
result_df_sorted.to_csv('query_output.csv', index=False)
