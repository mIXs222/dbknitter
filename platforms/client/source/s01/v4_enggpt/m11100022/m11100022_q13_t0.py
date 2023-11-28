# file: query_analysis.py
import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Retrieve customer data from MySQL
customer_query = "SELECT C_CUSTKEY FROM customer;"
customers_df = pd.read_sql(customer_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379)

# Retrieve orders data from Redis
orders_df_raw = redis_conn.get('orders')
orders_df = pd.read_json(orders_df_raw)

# Filter out orders with comments containing 'pending' or 'deposits'
orders_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', na=False)]

# Merge customer and orders dataframes on customer key
merged_df = pd.merge(customers_df, orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Group by customer key and count the number of orders
customer_orders_count = merged_df.groupby('C_CUSTKEY').size().reset_index(name='C_COUNT')

# Group by order count and calculate number of customers with that specific count
customer_distribution = customer_orders_count.groupby('C_COUNT').size().reset_index(name='CUSTDIST')

# Sort the results according to the specifications
sorted_distribution = customer_distribution.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write to CSV file
sorted_distribution.to_csv('query_output.csv', index=False)
