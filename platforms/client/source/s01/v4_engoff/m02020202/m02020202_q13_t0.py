import pymysql
import csv
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get orders data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT O_CUSTKEY, O_COMMENT FROM orders WHERE O_ORDERSTATUS NOT LIKE 'pending%' AND O_ORDERSTATUS NOT LIKE 'deposits%'")
    orders_data = cursor.fetchall()

# Convert orders data to Pandas DataFrame
orders_df = pd.DataFrame(orders_data, columns=['C_CUSTKEY', 'O_COMMENT'])

# Get customer data from Redis
customer_df = pd.read_json(redis_conn.get('customer'), orient='records')

# Group by customer key and count non-special categories in comment
orders_grouped = orders_df.groupby('C_CUSTKEY').agg({'O_COMMENT': lambda x: x[~x.str.contains('pending|deposits')].count()}).reset_index()
orders_grouped.columns = ['C_CUSTKEY', 'NUM_ORDERS']

# Merge customer data with orders data
result = customer_df.merge(orders_grouped, how='left', on='C_CUSTKEY')
result.fillna(0, inplace=True)

# Count the distribution
distribution = result.groupby('NUM_ORDERS').size().reset_index(name='NUM_CUSTOMERS')

# Write the output to a CSV file
distribution.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
redis_conn.close()
