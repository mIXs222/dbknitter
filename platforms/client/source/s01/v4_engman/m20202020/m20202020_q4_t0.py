import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Execute MySQL Query
sql_query = """SELECT L_ORDERKEY 
               FROM lineitem 
               WHERE L_COMMITDATE < L_RECEIPTDATE;"""
with mysql_conn.cursor() as cursor:
    cursor.execute(sql_query)
    late_orders = cursor.fetchall()

# Convert MySQL results into DataFrame
late_order_keys = [order[0] for order in late_orders]
df_late_orders = pd.DataFrame(late_order_keys, columns=['O_ORDERKEY'])

# Close MySQL connection
mysql_conn.close()

# Redis Connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get orders DataFrame from Redis
df_orders_redis = redis_conn.get('orders')
df_orders_redis['O_ORDERDATE'] = pd.to_datetime(df_orders_redis['O_ORDERDATE'])

# Filtering orders between specific dates
filtered_orders = df_orders_redis[(df_orders_redis['O_ORDERDATE'] >= '1993-07-01') &
                                  (df_orders_redis['O_ORDERDATE'] <= '1993-10-01')]

# Find late received orders
late_orders_df = filtered_orders[filtered_orders['O_ORDERKEY'].isin(df_late_orders['O_ORDERKEY'])]

# Count such orders for each order priority
order_priority_counts = late_orders_df.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

# Sort by order priority
order_priority_counts = order_priority_counts.sort_values(by='O_ORDERPRIORITY')

# Write to CSV
order_priority_counts.to_csv('query_output.csv', index=False)
