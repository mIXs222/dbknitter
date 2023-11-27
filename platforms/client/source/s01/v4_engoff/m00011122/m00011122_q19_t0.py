import pymysql
import pandas as pd
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Query to get relevant parts from MySQL
part_query = """
SELECT P_PARTKEY, P_BRAND, P_CONTAINER, P_SIZE FROM part
WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
   OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
   OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""

mysql_cursor.execute(part_query)
parts = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_BRAND', 'P_CONTAINER', 'P_SIZE'])

# Redis connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query to get relevant lineitems from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='split')

# Combine results
combined_df = pd.merge(parts, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')
combined_filtered_df = combined_df[
    ((combined_df['P_BRAND'] == 'Brand#12') & (combined_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (combined_df['L_QUANTITY'] >= 1) & (combined_df['L_QUANTITY'] <= 11) & (combined_df['P_SIZE'] >= 1) & (combined_df['P_SIZE'] <= 5)) |
    ((combined_df['P_BRAND'] == 'Brand#23') & (combined_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & (combined_df['L_QUANTITY'] >= 10) & (combined_df['L_QUANTITY'] <= 20) & (combined_df['P_SIZE'] >= 1) & (combined_df['P_SIZE'] <= 10)) |
    ((combined_df['P_BRAND'] == 'Brand#34') & (combined_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (combined_df['L_QUANTITY'] >= 20) & (combined_df['L_QUANTITY'] <= 30) & (combined_df['P_SIZE'] >= 1) & (combined_df['P_SIZE'] <= 15))
    & (combined_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']))
    & (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
]

# Calculate gross discounted revenue
combined_filtered_df['DISC_PRICE'] = combined_filtered_df['L_EXTENDEDPRICE'] * (1 - combined_filtered_df['L_DISCOUNT'])
result_df = combined_filtered_df
result_df.to_csv('query_output.csv', index=False)
