import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Read lineitem table from MySQL where L_SHIPDATE satisfies the condition
lineitem_query = """
SELECT L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT
FROM lineitem
WHERE L_SHIPDATE >= '1995-09-01'
AND L_SHIPDATE < '1995-10-01'
"""
lineitem_df = pd.read_sql(lineitem_query, mysql_connection)
mysql_connection.close()

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Read part table from Redis
part_df = pd.read_json(redis_connection.get('part').decode('utf-8'))

# Merge dataframes
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate PROMO_REVENUE
merged_df['VALUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
merged_df['PROMO_VALUE'] = merged_df.apply(
    lambda row: row['VALUE'] if row['P_TYPE'].startswith('PROMO') else 0,
    axis=1
)
promo_revenue = 100.00 * sum(merged_df['PROMO_VALUE']) / sum(merged_df['VALUE'])

# Write result to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PROMO_REVENUE'])
    writer.writerow([promo_revenue])
