import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = """
SELECT
    L_ORDERKEY,
    L_PARTKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_QUANTITY,
    L_SHIPMODE,
    L_SHIPINSTRUCT
FROM
    lineitem
WHERE
    L_SHIPMODE IN ('AIR', 'AIR REG')
    AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
"""

lineitem_df = pd.read_sql(mysql_query, con=mysql_conn)
mysql_conn.close()

# Redis connection and data retrieval
redis_conn = DirectRedis(host='redis', port=6379, db=0)
part_df = pd.read_json(redis_conn.get('part'))

# Filtering part dataframe based on brand, container and size
part_df = part_df[(part_df['P_BRAND'].isin(['Brand#12', 'Brand#23', 'Brand#34'])) &
                 (part_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
                 (part_df['P_SIZE'].between(1, 15))]

# Merge the lineitem and part dataframes on P_PARTKEY and L_PARTKEY
merged_df = lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter the merged dataframe based on the brand-specific conditions
conditions = [
    (merged_df['P_BRAND'] == 'Brand#12') & (merged_df['P_SIZE'].between(1, 5)) & (merged_df['L_QUANTITY'].between(1, 11)),
    (merged_df['P_BRAND'] == 'Brand#23') & (merged_df['P_SIZE'].between(1, 10)) & (merged_df['L_QUANTITY'].between(10, 20)),
    (merged_df['P_BRAND'] == 'Brand#34') & (merged_df['P_SIZE'].between(1, 15)) & (merged_df['L_QUANTITY'].between(20, 30))
]

# Filter rows that meet any of the conditions
merged_df = merged_df[(conditions[0]) | (conditions[1]) | (conditions[2])]
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Calculate the sum of REVENUE
revenue_sum = merged_df.groupby('L_ORDERKEY')['REVENUE'].sum().reset_index()

# Write result to csv
revenue_sum.to_csv('query_output.csv', index=False)
