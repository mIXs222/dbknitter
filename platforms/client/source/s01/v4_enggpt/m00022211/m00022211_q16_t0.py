# query.py
import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL and retrieve part table
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
part_query = """
    SELECT P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT
    FROM part
    WHERE P_BRAND <> 'Brand#45'
      AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
      AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
"""
part_df = pd.read_sql(part_query, mysql_conn)
mysql_conn.close()

# Connect to Redis and retrieve partsupp and supplier tables
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
partsupp_df = pd.read_json(redis_conn.get('partsupp'))
supplier_df = pd.read_json(redis_conn.get('supplier'))

redis_conn.close()

# Filter out suppliers with 'Customer Complaints' in the comments
supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]

# Merge the dataframes on matching keys
merged_df = partsupp_df.merge(part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group and count distinct suppliers
grouped_df = (
    merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
    .agg(SUPPLIER_CNT=pd.NamedAgg(column='S_SUPPKEY', aggfunc='nunique'))
    .reset_index()
)

# Sort the results as specified
sorted_df = grouped_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write the output to query_output.csv
sorted_df.to_csv('query_output.csv', index=False)
