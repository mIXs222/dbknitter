import pymysql
import pandas as pd
import direct_redis
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Fetch lineitem data from MySQL
mysql_query = """
SELECT L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
FROM lineitem
WHERE L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE < '1995-10-01'
"""
lineitem_df = pd.read_sql(mysql_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch part data from Redis
part_keys = ["P_PARTKEY", "P_TYPE"]
part_data = {}
for key in part_keys:
    part_data[key] = redis_conn.get(key)
    if part_data[key] is not None:
        part_data[key] = pd.read_json(part_data[key], orient='split')

# Combine part columns into a single DataFrame
part_df = pd.DataFrame(part_data)

# Merge the DataFrames
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate PROMO_REVENUE
promo_revenue_df = merged_df.assign(
    Revenue=lambda x: x.L_EXTENDEDPRICE * (1 - x.L_DISCOUNT)
).assign(
    PromoRevenue=lambda x: x.Revenue.where(merged_df.P_TYPE.str.startswith('PROMO'), 0)
)

promo_revenue_result = promo_revenue_df.PromoRevenue.sum() / promo_revenue_df.Revenue.sum() * 100.00
promo_revenue = [{'PROMO_REVENUE': promo_revenue_result}]

# Write result to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['PROMO_REVENUE'])
    writer.writeheader()
    writer.writerows(promo_revenue)

