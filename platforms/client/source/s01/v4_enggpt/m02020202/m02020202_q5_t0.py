import pandas as pd
import pymysql
from datetime import datetime
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

try:
    # Load data from MySQL
    nation_query = "SELECT * FROM nation;"
    orders_query = "SELECT * FROM orders WHERE O_ORDERDATE >= '1990-01-01' AND O_ORDERDATE <= '1994-12-31';"
    
    df_nation = pd.read_sql(nation_query, mysql_conn)
    df_orders = pd.read_sql(orders_query, mysql_conn)
    
finally:
    mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load Redis data
df_region = pd.read_json(redis_conn.get('region'))
df_supplier = pd.read_json(redis_conn.get('supplier'))
df_customer = pd.read_json(redis_conn.get('customer'))
df_lineitem = pd.read_json(redis_conn.get('lineitem'))

# Combine the data
df_nation_region = df_nation.merge(df_region[df_region.R_NAME == 'ASIA'], left_on='N_REGIONKEY', right_on='R_REGIONKEY')
df_customer_nation = df_customer.merge(df_nation_region, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
df_orders_customer = df_orders.merge(df_customer_nation, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
df_lineitem_orders = df_lineitem.merge(df_orders_customer, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df_supplier_nation = df_supplier.merge(df_nation_region, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df_final = df_lineitem_orders.merge(df_supplier_nation, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Compute the revenue
df_final['revenue'] = df_final['L_EXTENDEDPRICE'] * (1 - df_final['L_DISCOUNT'])
df_revenue_by_nation = df_final.groupby(['N_NAME'], as_index=False).agg({'revenue': 'sum'})

# Sort by revenue in descending order
df_revenue_sorted = df_revenue_by_nation.sort_values(by='revenue', ascending=False)

# Write to CSV file
df_revenue_sorted.to_csv('query_output.csv', index=False)
