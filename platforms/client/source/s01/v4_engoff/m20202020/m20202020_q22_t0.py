import pymysql
import pandas as pd
from datetime import datetime, timedelta
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
with mysql_conn.cursor() as cursor:
    query = """
    SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_COMMENT
    FROM customer 
    WHERE C_ACCTBAL > 0.00 AND SUBSTRING(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21');
    """
    cursor.execute(query)
    customers = cursor.fetchall()

# Process customer data
customer_df = pd.DataFrame(customers, columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT'])

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
orders_df_raw = redis_conn.get('orders')
orders_df = pd.read_json(orders_df_raw)

# Process orders data by filtering orders that are not older than 7 years
seven_years_ago = (datetime.now() - timedelta(days=7*365)).date()
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
recent_orders_df = orders_df[orders_df['O_ORDERDATE'] >= seven_years_ago]

# Merge customer data with recent orders to find customers who have not placed orders in the last 7 years
result_df = pd.merge(customer_df, recent_orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result_df = result_df[result_df['O_ORDERDATE'].isna()].drop(columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])
result_df = result_df.groupby(['C_NATIONKEY']).agg({'C_CUSTKEY': 'count', 'C_ACCTBAL': 'mean'}).reset_index()
result_df.columns = ['C_NATIONKEY', 'CUSTOMER_COUNT', 'AVG_ACCTBAL']

# Output results to CSV
result_df.to_csv('query_output.csv', index=False)
