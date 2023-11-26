import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_query = """
SELECT P_PARTKEY, P_BRAND, P_CONTAINER, P_SIZE 
FROM part
WHERE 
    (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5) OR
    (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10) OR
    (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""
part_df = pd.read_sql(mysql_query, conn)
conn.close()

# Redis connection and data retrieval
direct_redis = DirectRedis(host='redis', port=6379, db=0)
lineitem = direct_redis.get('lineitem')
lineitem_df = pd.read_msgpack(lineitem)

# Query execution logic
result_df = lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
result_df = result_df[
    (
        (result_df['L_QUANTITY'] >= 1) & (result_df['L_QUANTITY'] <= 11) &
        (result_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
        (result_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    ) |
    (
        (result_df['L_QUANTITY'] >= 10) & (result_df['L_QUANTITY'] <= 20) &
        (result_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
        (result_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    ) |
    (
        (result_df['L_QUANTITY'] >= 20) & (result_df['L_QUANTITY'] <= 30) &
        (result_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
        (result_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    )
]

# Calculating revenue
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])
revenue = result_df['REVENUE'].sum()

# Output to csv
output_df = pd.DataFrame({'REVENUE': [revenue]})
output_df.to_csv('query_output.csv', index=False)
