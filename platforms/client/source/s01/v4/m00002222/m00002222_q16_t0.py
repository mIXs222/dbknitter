# query.py

import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute the query for the 'part' table
part_query = """SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE
    FROM part
    WHERE P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)"""
part_df = pd.read_sql(part_query, mysql_connection)

# Execute the subquery for 'supplier' table
supplier_query = """SELECT S_SUPPKEY
    FROM supplier
    WHERE S_COMMENT LIKE '%Customer%Complaints%'"""
supplier_df = pd.read_sql(supplier_query, mysql_connection)

# Close MySQL connection
mysql_connection.close()

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
partsupp_df = pd.DataFrame(redis_connection.get('partsupp'))

# Manipulations to fit the partsupp dataframe format to the expected SQL-like structure
partsupp_df = partsupp_df.apply(lambda x: pd.Series(x.dropna().values))
partsupp_df.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']
partsupp_df['PS_PARTKEY'] = partsupp_df['PS_PARTKEY'].astype(int)
partsupp_df['PS_SUPPKEY'] = partsupp_df['PS_SUPPKEY'].astype(int)

# Cast the key columns to int for proper merging
part_df['P_PARTKEY'] = part_df['P_PARTKEY'].astype(int)

# Merge the DataFrames
merged_df = pd.merge(
    part_df, partsupp_df,
    how='inner',
    left_on='P_PARTKEY',
    right_on='PS_PARTKEY'
)

# Filter out suppliers
filtered_df = merged_df[~merged_df['PS_SUPPKEY'].isin(supplier_df['S_SUPPKEY'])]

# Grouping and aggregation
result_df = filtered_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique')).reset_index()

# Sorting the results
result_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Write output to a CSV file
result_df.to_csv('query_output.csv', index=False)
