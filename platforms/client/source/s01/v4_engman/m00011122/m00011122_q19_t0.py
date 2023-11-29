# query.py
import pymysql
import pandas as pd
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Prepare MySQL query
mysql_query = '''
SELECT P_PARTKEY FROM part
WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
   OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
   OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
'''

# Fetch MySQL data
mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Redis connection
redis_conn = direct_redis.DirectRedis(
    host='redis',
    port=6379,
    db=0,
)

# Fetch Redis data
lineitem_df = redis_conn.get('lineitem')

# Filter Redis data
filtered_lineitem_df = lineitem_df[
    ((lineitem_df['L_SHIPMODE'] == 'AIR') | (lineitem_df['L_SHIPMODE'] == 'AIR REG')) &
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON') &
    (((lineitem_df['L_QUANTITY'] >= 1) & (lineitem_df['L_QUANTITY'] <= 11)) |
     ((lineitem_df['L_QUANTITY'] >= 10) & (lineitem_df['L_QUANTITY'] <= 20)) |
     ((lineitem_df['L_QUANTITY'] >= 20) & (lineitem_df['L_QUANTITY'] <= 30)))
]

# Merge data frames to get the discounted revenue
discounted_revenue_df = filtered_lineitem_df[
    filtered_lineitem_df['L_PARTKEY'].isin(mysql_df['P_PARTKEY'])
]
discounted_revenue_df['REVENUE'] = discounted_revenue_df['L_EXTENDEDPRICE'] * (1 - discounted_revenue_df['L_DISCOUNT'])
result = pd.DataFrame([{'REVENUE': discounted_revenue_df['REVENUE'].sum()}])

# Write output to CSV
result.to_csv('query_output.csv', index=False)
