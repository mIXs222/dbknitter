import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379)

# Query MySQL for line items where the commitment date precedes the receipt date.
mysql_query = """
SELECT DISTINCT L_ORDERKEY
FROM lineitem
WHERE L_COMMITDATE < L_RECEIPTDATE
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    lineitem_keys = cursor.fetchall()

lineitem_orderkeys = [row[0] for row in lineitem_keys]

# Fetching orders data from Redis
orders_df = pd.DataFrame(redis_client.get('orders'))
orders_df.columns = ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']

# Converting 'O_ORDERDATE' to datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filtering the orders based on date criteria
orders_filtered_df = orders_df[
    (orders_df['O_ORDERDATE'] >= pd.Timestamp(1993, 7, 1)) &
    (orders_df['O_ORDERDATE'] <= pd.Timestamp(1993, 10, 1))
]

# Merging with line items on O_ORDERKEY after filtering for lineitems with the commitment-receipt date condition
valid_orders_df = orders_filtered_df[orders_filtered_df['O_ORDERKEY'].isin(lineitem_orderkeys)]

# Group by the order priority with the count
order_priority_counts = valid_orders_df.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index()
order_priority_counts = order_priority_counts.rename(columns={'O_ORDERKEY': 'COUNT'})

# Sorting by the order priority
order_priority_counts_sorted = order_priority_counts.sort_values(by='O_ORDERPRIORITY')

# Writing out to 'query_output.csv'
order_priority_counts_sorted.to_csv('query_output.csv', index=False)

# Closing database connections
mysql_conn.close()
