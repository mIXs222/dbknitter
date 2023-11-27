# query.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis
from datetime import datetime

# Establish connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Query MySQL to get order details
with mysql_conn.cursor() as cursor:
    query = '''
    SELECT
        o.O_ORDERKEY,
        o.O_SHIPPRIORITY,
        SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue
    FROM
        orders o
    JOIN
        lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
    WHERE
        l.L_SHIPDATE > '1995-03-15'
    GROUP BY
        o.O_ORDERKEY
    ORDER BY
        revenue DESC;
    '''
    cursor.execute(query)
    orders_result = cursor.fetchall()

# Transform MySQL data to DataFrame
orders_df = pd.DataFrame(orders_result, columns=['O_ORDERKEY', 'O_SHIPPRIORITY', 'revenue'])

# Establish connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get customer data from Redis
customer_data = redis_client.get('customer')
customers_df = pd.read_json(customer_data)

# Convert to DataFrame if returned as a string
if isinstance(customers_df, str):
    customers_df = pd.read_json(customers_df)

# Filtering customers with 'BUILDING' market segment
building_customers_df = customers_df[customers_df['C_MKTSEGMENT'] == 'BUILDING']

# Merge orders and customers data on 'C_CUSTKEY' and 'O_CUSTKEY' respectively
merged_df = orders_df.merge(building_customers_df, how='inner', left_on='O_ORDERKEY', right_on='C_CUSTKEY')

# Get the top orders by revenue
top_orders_df = merged_df.nlargest(1, 'revenue')

# Select required columns and write to csv
result_df = top_orders_df[['O_ORDERKEY', 'O_SHIPPRIORITY', 'revenue']]
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
