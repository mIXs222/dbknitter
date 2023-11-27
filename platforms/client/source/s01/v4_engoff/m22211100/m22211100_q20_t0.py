# Python code to execute the query on different databases and merge results
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = """
SELECT l.L_PARTKEY, s.S_SUPPKEY
FROM lineitem l
JOIN (SELECT S_SUPPKEY, S_NATIONKEY FROM supplier) s ON l.L_SUPPKEY = s.S_SUPPKEY
WHERE l.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
"""
lineitems_suppliers = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# MongoDB connection and query
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']
partsupp_collection = mongodb_db['partsupp']
partsupp_docs = partsupp_collection.find()
partsupp_df = pd.DataFrame(list(partsupp_docs))

# Redis connection and retrieval of data
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.DataFrame(redis_conn.get('nation'))
nation_df.columns = nation_df.iloc[0] 
nation_df = nation_df[1:]
part_df = pd.DataFrame(redis_conn.get('part'))
part_df.columns = part_df.iloc[0]
part_df = part_df[1:]

# Merge the dataframes
merged_df = lineitems_suppliers.merge(partsupp_df, how='inner', left_on=['L_PARTKEY', 'S_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
merged_df = merged_df.merge(nation_df[nation_df['N_NAME'] == 'CANADA'], how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df[merged_df['P_NAME'].str.contains('forest', case=False)]

# Apply the condition for excess of forest part
supplier_part_count = merged_df.groupby('S_SUPPKEY')['L_PARTKEY'].count().reset_index()
supplier_excess_parts = supplier_part_count[supplier_part_count['L_PARTKEY'] > (supplier_part_count['L_PARTKEY'].sum() * 0.5)]
result_df = supplier_excess_parts[['S_SUPPKEY']].drop_duplicates()

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
