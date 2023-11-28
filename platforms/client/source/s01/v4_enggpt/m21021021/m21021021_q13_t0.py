import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for customer data
customer_query = "SELECT C_CUSTKEY, C_NAME FROM customer;"
with mysql_conn.cursor() as cursor:
    cursor.execute(customer_query)
    customer_data = cursor.fetchall()

customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NAME'])
mysql_conn.close()

# Get Redis orders data
orders_df = pd.read_json(redis_conn.get('orders'))

# Filter out orders with 'pending' and 'deposits' in comments
orders_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', na=False)]

# Merge dataframes
merged_df = pd.merge(customer_df, orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Subquery equivalent
grouped = merged_df.groupby('C_CUSTKEY')
aggregate_df = grouped['O_ORDERKEY'].agg(C_COUNT='count').reset_index()

# Outer query equivalent
customer_distribution_df = aggregate_df.groupby('C_COUNT').agg(CUSTDIST=('C_COUNT', 'count')).reset_index()

# Sort the results
customer_distribution_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)

# Write to CSV
customer_distribution_df.to_csv('query_output.csv', index=False)

# Close Redis connection
redis_conn.close()
