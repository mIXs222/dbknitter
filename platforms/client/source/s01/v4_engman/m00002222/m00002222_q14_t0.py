import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Retrieve data from MySQL 'part' table where 'P_NAME' indicates a promotional part
promotion_parts_query = """
SELECT P_PARTKEY
FROM part
WHERE P_NAME like '%Promo%'
"""
mysql_cursor.execute(promotion_parts_query)
promotion_parts = mysql_cursor.fetchall()
promotion_part_keys = [row[0] for row in promotion_parts]

mysql_cursor.close()
mysql_conn.close()

# Establish connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis 'lineitem' table
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Filter for line items corresponding to the promotion parts and within the date range
promotion_lineitems = lineitem_df[
    lineitem_df['L_PARTKEY'].isin(promotion_part_keys) &
    (lineitem_df['L_SHIPDATE'] >= '1995-09-01') &
    (lineitem_df['L_SHIPDATE'] <= '1995-10-01')
]

# Calculate revenue
promotion_lineitems['revenue'] = promotion_lineitems['L_EXTENDEDPRICE'] * (1 - promotion_lineitems['L_DISCOUNT'])

# Calculate total revenue for the promotion period
total_promotion_revenue = promotion_lineitems['revenue'].sum()

# Write the result to query_output.csv
with open('query_output.csv', 'w') as outfile:
    outfile.write('total_promotion_revenue\n')
    outfile.write(f'{total_promotion_revenue}\n')
