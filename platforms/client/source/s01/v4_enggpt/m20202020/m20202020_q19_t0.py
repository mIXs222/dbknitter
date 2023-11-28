import pymysql
import direct_redis
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Retrieving data from MySQL
query = """
SELECT *
FROM lineitem
WHERE (
    (L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON' AND L_QUANTITY BETWEEN 1 AND 11)
    OR (L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON' AND L_QUANTITY BETWEEN 10 AND 20)
    OR (L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON' AND L_QUANTITY BETWEEN 20 AND 30)
)
"""
lineitem_df = pd.read_sql(query, mysql_conn)
mysql_conn.close()

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieving part data from Redis
part_df = pd.read_json(r.get('part'), orient='records')

# Merge DataFrames
results = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filtering merged DataFrame by conditions
filtered_results = results[
    ((results['P_BRAND'] == 'Brand#12') & (results['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (results['L_QUANTITY'] >= 1) & (results['L_QUANTITY'] <= 11) & (results['P_SIZE'] >= 1) & (results['P_SIZE'] <= 5)) |
    ((results['P_BRAND'] == 'Brand#23') & (results['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & (results['L_QUANTITY'] >= 10) & (results['L_QUANTITY'] <= 20) & (results['P_SIZE'] >= 1) & (results['P_SIZE'] <= 10)) |
    ((results['P_BRAND'] == 'Brand#34') & (results['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (results['L_QUANTITY'] >= 20) & (results['L_QUANTITY'] <= 30) & (results['P_SIZE'] >= 1) & (results['P_SIZE'] <= 15))
]

# Calculate revenue
filtered_results['REVENUE'] = filtered_results['L_EXTENDEDPRICE'] * (1 - filtered_results['L_DISCOUNT'])

# Group by specified criteria and calculate total revenue
grouped_revenue = filtered_results.groupby(['P_BRAND', 'P_CONTAINER', 'L_QUANTITY', 'P_SIZE', 'L_SHIPMODE', 'L_SHIPINSTRUCT'])['REVENUE'].sum().reset_index()

# Output results to CSV
grouped_revenue.to_csv('query_output.csv', index=False)
