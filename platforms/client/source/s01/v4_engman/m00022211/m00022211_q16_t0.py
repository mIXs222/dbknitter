# query.py
import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Query parts from MySQL database
part_query = """SELECT P_PARTKEY, P_TYPE, P_SIZE, P_BRAND
                FROM part
                WHERE P_BRAND <> 'Brand#45'
                AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
                AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9);"""
parts_df = pd.read_sql(part_query, mysql_connection)
mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get supplier and partsupp tables from Redis
supplier_df = pd.DataFrame(redis_connection.get('supplier'))
partsupp_df = pd.DataFrame(redis_connection.get('partsupp'))

# Filter out suppliers with complaints
supplier_df = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]

# Join DataFrames to get the final output
result_df = parts_df.merge(partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
result_df = result_df.merge(supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Count the number of suppliers that satisfy conditions
final_result_df = result_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg({'S_SUPPKEY': 'count'}).reset_index()
final_result_df = final_result_df.rename(columns={'S_SUPPKEY': 'SUPPLIER_COUNT'})
final_result_df.sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Write the output to CSV
final_result_df.to_csv('query_output.csv', index=False)
