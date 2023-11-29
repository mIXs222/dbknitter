import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
# Prepare the MySQL query
mysql_query = """
SELECT
    S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
FROM
    supplier
JOIN
    lineitem ON supplier.S_SUPPKEY = lineitem.L_SUPPKEY
WHERE
    L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE < '1996-04-01'
GROUP BY
    S_SUPPKEY
ORDER BY
    TOTAL_REVENUE DESC, S_SUPPKEY
"""

# Read from MySQL
supplier_revenue_df = pd.read_sql(mysql_query, mysql_conn)
# Close the MySQL connection
mysql_conn.close()

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
# Read the lineitem table as a CSV string from Redis
lineitem_csv = redis_client.get('lineitem')
# Convert the CSV string to a DataFrame
lineitem_df = pd.read_csv(pd.compat.StringIO(lineitem_csv))

# Merge the data from Redis with the MySQL DataFrame
merged_df = supplier_revenue_df.merge(lineitem_df, on='L_SUPPKEY')

# Calculate the top suppliers by total revenue
top_suppliers_df = merged_df.groupby(
    ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE']
).agg({
    'TOTAL_REVENUE': 'max'
}).reset_index()

# Sort by total revenue and suppkey
top_suppliers_df = top_suppliers_df.sort_values(by=['TOTAL_REVENUE', 'S_SUPPKEY'], ascending=[False, True])

# Get the highest revenue
max_revenue = top_suppliers_df['TOTAL_REVENUE'].max()

# Filter for suppliers tied for the highest revenue
top_suppliers_df = top_suppliers_df[top_suppliers_df['TOTAL_REVENUE'] == max_revenue]

# Write to CSV
top_suppliers_df.to_csv('query_output.csv', index=False)
