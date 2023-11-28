import pymysql
import pandas as pd
import direct_redis
from datetime import datetime

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Getting data from MySQL
query_customer = """
SELECT C_CUSTKEY, C_MKTSEGMENT FROM customer WHERE C_MKTSEGMENT = 'BUILDING';
"""
query_lineitem = """
SELECT L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE FROM lineitem WHERE L_SHIPDATE > '1995-03-15';
"""
customers = pd.read_sql(query_customer, mysql_conn)
lineitems = pd.read_sql(query_lineitem, mysql_conn)

mysql_conn.close()

# Redis connection setup and getting data
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
orders_df_json = redis_client.get('orders')
orders = pd.read_json(orders_df_json)

# Filtering orders before '1995-03-15'
orders_before_date = orders[orders['O_ORDERDATE'] < datetime(1995, 3, 15)]

# Merging the dataframes to simulate a JOIN
customer_orders = pd.merge(customers, orders_before_date, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result = pd.merge(customer_orders, lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculating revenue after discounts
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Grouping by specified columns
grouped = result.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({'REVENUE': 'sum'}).reset_index()

# Sorting as per the query - by revenue descending and order date ascending
sorted_grouped = grouped.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Writing to file
sorted_grouped.to_csv('query_output.csv', index=False)
