import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Fetch lineitem data from MySQL between the specified dates
query_mysql = """
    SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
    FROM lineitem
    WHERE L_SHIPDATE BETWEEN '1996-01-01' AND '1996-04-01'
    GROUP BY L_SUPPKEY
"""

lineitem_df = pd.read_sql(query_mysql, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch supplier data from Redis
supplier_json = redis_client.get('supplier')
supplier_df = pd.read_json(supplier_json, orient='records')

# Merge MySQL and Redis data
merged_df = pd.merge(supplier_df, lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Get the maximum total revenue
max_revenue = merged_df['total_revenue'].max()

# Find the supplier with the maximum revenue
top_suppliers_df = merged_df[merged_df['total_revenue'] == max_revenue]

# Sort by supplier key - S_SUPPKEY (asc)
top_suppliers_sorted_df = top_suppliers_df.sort_values(by=['S_SUPPKEY'])

# Write result to CSV
top_suppliers_sorted_df.to_csv('query_output.csv', index=False)
