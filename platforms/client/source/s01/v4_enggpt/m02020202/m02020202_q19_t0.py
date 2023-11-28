import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Establish connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Establish connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get part table from mysql
mysql_query = """
SELECT *
FROM part
WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
   OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
   OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""
part_df = pd.read_sql(mysql_query, mysql_conn)

# Get lineitem table from redis
lineitem_df = pd.DataFrame(redis_conn.get('lineitem'))

# Perform analysis
query_result = part_df.merge(lineitem_df, how='inner', left_on='P_PARTKEY', right_on='L_PARTKEY')

conditions = [
    (query_result['P_BRAND'] == 'Brand#12') & (query_result['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']))
    & (query_result['L_QUANTITY'].between(1, 11)) & (query_result['P_SIZE'].between(1, 5))
    & (query_result['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (query_result['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),
    
    (query_result['P_BRAND'] == 'Brand#23') & (query_result['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']))
    & (query_result['L_QUANTITY'].between(10, 20)) & (query_result['P_SIZE'].between(1, 10))
    & (query_result['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (query_result['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),
    
    (query_result['P_BRAND'] == 'Brand#34') & (query_result['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']))
    & (query_result['L_QUANTITY'].between(20, 30)) & (query_result['P_SIZE'].between(1, 15))
    & (query_result['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) & (query_result['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
]

query_result['SELECTED'] = any(conditions)

revenue_df = query_result[query_result['SELECTED']]
revenue_df['REVENUE'] = revenue_df['L_EXTENDEDPRICE'] * (1 - revenue_df['L_DISCOUNT'])

total_revenue = revenue_df.groupby('P_PARTKEY')['REVENUE'].sum().reset_index()

# Write to CSV file
total_revenue.to_csv('query_output.csv', index=False)

# Clean up connections
mysql_conn.close()
redis_conn.close()
