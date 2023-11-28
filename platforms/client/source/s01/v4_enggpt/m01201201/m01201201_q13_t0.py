# query_analysis.py
import pymysql
import pandas as pd
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Perform the query on the MySQL database
mysql_cursor.execute("""
    SELECT o.O_CUSTKEY, COUNT(o.O_ORDERKEY) AS C_COUNT
    FROM orders o
    WHERE o.O_COMMENT NOT LIKE '%pending%' AND o.O_COMMENT NOT LIKE '%deposit%'
    GROUP BY o.O_CUSTKEY
""")
orders_data = mysql_cursor.fetchall()

# Convert to DataFrame
orders_df = pd.DataFrame(list(orders_data), columns=['C_CUSTKEY', 'C_COUNT'])

# Redis connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis
customer_df = pd.read_json(redis_conn.get('customer'), orient='records')

# Merge dataframes on C_CUSTKEY
merged_df = pd.merge(customer_df, orders_df, how='left', on='C_CUSTKEY').fillna(0)
# Compute distribution
dist_df = merged_df.groupby('C_COUNT', as_index=False).agg(CUSTDIST=('C_CUSTKEY', 'count'))
# Order the results
dist_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)

# Write results to CSV
dist_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
