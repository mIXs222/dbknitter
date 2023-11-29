import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)
mysql_cursor = mysql_conn.cursor()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve customer data from Redis
customer_data = redis_conn.get('customer')
customer_df = pd.read_json(customer_data)

# Filter customers with market segment "BUILDING"
building_customers = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

# MySQL query
mysql_query = """
SELECT
    o.O_ORDERKEY,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
    o.O_ORDERDATE,
    o.O_SHIPPRIORITY
FROM
    orders o
JOIN
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE
    o.O_ORDERDATE < '1995-03-05'
    AND l.L_SHIPDATE > '1995-03-15'
    AND o.O_CUSTKEY IN %s
GROUP BY
    o.O_ORDERKEY, o.O_ORDERDATE, o.O_SHIPPRIORITY
ORDER BY
    REVENUE DESC
"""

# Execute MySQL query
mysql_cursor.execute(mysql_query, [tuple(building_customers['C_CUSTKEY'].tolist())])
result = mysql_cursor.fetchall()

# Write result to CSV file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    for row in result:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
