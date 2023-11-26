import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Retrieve customer data from MySQL
query_customer = "SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT FROM customer"
customer_df = pd.read_sql(query_customer, mysql_conn)

# Disconnect MySQL
mysql_conn.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve orders data from Redis
orders_str = redis_client.get('orders')
orders_df = pd.read_json(orders_str)

# Join the datasets and perform the query
result = (
    customer_df.merge(orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .query('~O_COMMENT.str.contains("pending.+deposits")', engine='python')
    .groupby('C_CUSTKEY')
    .agg(C_COUNT=('O_ORDERKEY', 'count'))
    .reset_index()
    .groupby('C_COUNT')
    .agg(CUSTDIST=('C_CUSTKEY', 'count'))
    .reset_index()
    .sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])
)

# Save to file
result.to_csv('query_output.csv', index=False)
