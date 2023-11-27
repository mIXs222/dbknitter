# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = """
    SELECT P_PARTKEY, P_RETAILPRICE FROM part
"""
part_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_con = DirectRedis(host='redis', port=6379, db=0)
lineitem_str = redis_con.get('lineitem')
lineitem_df = pd.read_json(lineitem_str)

# Filter lineitems by date
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= pd.Timestamp('1995-09-01')) &
    (lineitem_df['L_SHIPDATE'] <= pd.Timestamp('1995-10-01'))
]

# Compute Revenue
lineitem_df['revenue'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Merge DataFrames on part keys
merged_df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate total revenue and revenue from promotional parts
total_revenue = merged_df['revenue'].sum()

# Assuming "promotional parts" is a characteristic encoded in P_COMMENT
# This may vary and might require an actual flag or additional information
# to accurately determine whether a part is promotional
is_promotion = merged_df['P_COMMENT'].str.contains('promo', case=False, na=False)
promo_revenue = merged_df[is_promotion]['revenue'].sum()

# Calculate percentage
promo_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Write to CSV file
with open("query_output.csv", "w") as file:
    file.write(f"Promotion Effect, {promo_percentage}")

print("Query executed and saved to query_output.csv.")
