# Import necessary libraries
import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Fetch orders data from MySQL
orders_query = """SELECT O_ORDERKEY, O_ORDERPRIORITY
                  FROM orders
                  WHERE O_ORDERPRIORITY = '1-URGENT' OR O_ORDERPRIORITY = '2-HIGH';"""
orders = pd.read_sql(orders_query, mysql_conn)

# Close the MySQL connection
mysql_conn.close()

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch lineitem data from Redis
lineitem_df = r.get('lineitem')

# Convert JSON data to DataFrame
lineitem = pd.read_json(lineitem_df)

# Filter lineitem DataFrame
lineitem = lineitem[
    (lineitem['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (lineitem['L_COMMITDATE'] < lineitem['L_RECEIPTDATE']) &
    (lineitem['L_SHIPDATE'] < lineitem['L_COMMITDATE']) &
    (lineitem['L_RECEIPTDATE'] >= '1994-01-01') &
    (lineitem['L_RECEIPTDATE'] < '1995-01-01')
]

# Merge orders and lineitem data on O_ORDERKEY
merged_df = orders.merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Perform the aggregation
result = merged_df.groupby('L_SHIPMODE').apply(
    lambda df: pd.Series({
        'HIGH_LINE_COUNT': ((df['O_ORDERPRIORITY'] == '1-URGENT') | (df['O_ORDERPRIORITY'] == '2-HIGH')).sum(),
        'LOW_LINE_COUNT': ((df['O_ORDERPRIORITY'] != '1-URGENT') & (df['O_ORDERPRIORITY'] != '2-HIGH')).sum(),
    })
).reset_index()

# Write the result to a CSV file
result.to_csv('query_output.csv', index=False)
