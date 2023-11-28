# python code (query.py)

import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# Fetch the customer data 
customer_query = "SELECT * FROM customer;"
with mysql_conn.cursor() as cursor:
    cursor.execute(customer_query)
    mysql_customer_data = cursor.fetchall()
    df_customer = pd.DataFrame(mysql_customer_data, columns=[
        'C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 
        'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'
    ])

mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the orders data
redis_orders_data = redis_conn.get('orders')
df_orders = pd.read_json(redis_orders_data)

# Filter out orders with 'pending' or 'deposits' in the comments
filtered_orders = df_orders[~df_orders['O_COMMENT'].astype(str).str.contains('pending|deposits')]

# Conduct analyses on combined data
combined_df = pd.merge(
    df_customer,
    filtered_orders,
    how='left',
    left_on='C_CUSTKEY',
    right_on='O_CUSTKEY'
)

# Group by C_CUSTKEY to count the orders per customer
combined_df['C_COUNT'] = combined_df.groupby('C_CUSTKEY')['O_ORDERKEY'].transform('count')

# Generate the distribution of customers based on their order counts
distribution_df = combined_df[['C_CUSTKEY', 'C_COUNT']].drop_duplicates()
distribution_df = distribution_df['C_COUNT'].value_counts().reset_index()
distribution_df.columns = ['C_COUNT', 'CUSTDIST']

# Order the results by CUSTDIST and C_COUNT
distribution_df = distribution_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write to CSV
distribution_df.to_csv('query_output.csv', index=False)
