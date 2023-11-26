import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Get part data from MySQL database
mysql_query = """
SELECT
    P_PARTKEY,
    P_BRAND,
    P_TYPE,
    P_SIZE
FROM
    part
WHERE
    P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
"""
part_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get partsupp data from Redis
partsupp_df = pd.read_json(r.get('partsupp'), orient='records')

# Get supplier data from Redis
supplier_df = pd.read_json(r.get('supplier'), orient='records')

# Filter out suppliers with 'Customer%Complaints%'
supplier_df = supplier_df[~supplier_df["S_COMMENT"].str.contains('Customer.*Complaints')]

# Merge part and partsupp dataframes on part key
merged_df = pd.merge(part_df, partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Left join merged data with supplier data on suppkey
result_df = pd.merge(merged_df, supplier_df[['S_SUPPKEY']], how='left', left_on='PS_SUPPKEY', right_on='S_SUPPKEY', indicator=True)

# Filter rows that have matched supplier
result_df = result_df[result_df['_merge'] == 'left_only'].drop(['PS_PARTKEY', '_merge'], axis=1)

# Group by brand, type, and size and count distinct suppkey
final_result = result_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']) \
                        .agg(SUPPLIER_CNT=('PS_SUPPKEY', pd.Series.nunique)) \
                        .reset_index()

# Sort the result as specified in the query
final_result = final_result.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write the result to csv file
final_result.to_csv('query_output.csv', index=False)
