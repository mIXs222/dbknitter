import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
orders_query = "SELECT * FROM orders WHERE O_COMMENT NOT LIKE '%pending%deposits%'"
orders_df = pd.read_sql(orders_query, mysql_conn)
mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
customer_json = redis_conn.get('customer')
customer_df = pd.read_json(customer_json)

# Perform the JOIN operation in pandas (replacing the SQL JOIN)
merged_df = customer_df.merge(orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Perform the GROUP BY and COUNT operation in pandas (replacing the SQL GROUP BY and COUNT)
grouped = merged_df.groupby('C_CUSTKEY').agg(C_COUNT=('O_ORDERKEY', 'count')).reset_index()
cust_dist = grouped.groupby('C_COUNT').C_COUNT.count().reset_index(name='CUSTDIST')

# Perform the ORDER BY operation in pandas (replacing the SQL ORDER BY)
result = cust_dist.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write the output to a CSV file
result.to_csv('query_output.csv', index=False)
