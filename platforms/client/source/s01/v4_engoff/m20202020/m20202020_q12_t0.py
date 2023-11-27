import pymysql
import pandas as pd
from datetime import datetime
import direct_redis

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = mysql_conn.cursor()

# MySQL query
mysql_query = """
SELECT 
    L_SHIPMODE,
    L_ORDERKEY,
    L_RECEIPTDATE,
    L_COMMITDATE,
    L_SHIPDATE
FROM 
    lineitem
WHERE
    L_SHIPMODE IN ('MAIL', 'SHIP') AND
    L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01' AND
    L_SHIPDATE < L_COMMITDATE
"""
cursor.execute(mysql_query)
lineitem_records = cursor.fetchall()

# Cast to DataFrame
lineitem_df = pd.DataFrame(lineitem_records, columns=['L_SHIPMODE', 'L_ORDERKEY', 'L_RECEIPTDATE', 'L_COMMITDATE','L_SHIPDATE'])

# Connection to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetching data from Redis and casting to DataFrame
orders_df = pd.read_json(redis_conn.get('orders'))

# Merging datasets
merged_data = pd.merge(lineitem_df, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Filtering data
result = merged_data[
    (merged_data['L_RECEIPTDATE'] > merged_data['L_COMMITDATE']) &
    (merged_data['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH']) | ~merged_data['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH']))
]

# Aggregating results
final_result = result.groupby(['L_SHIPMODE', 'O_ORDERPRIORITY']).size().reset_index(name='LATE_LINEITEMS')

# Writing output to a CSV file
final_result.to_csv('query_output.csv', index=False)

# Closing database connections
cursor.close()
mysql_conn.close()
