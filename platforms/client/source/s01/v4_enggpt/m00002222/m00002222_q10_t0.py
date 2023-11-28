import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query the MySQL 'nation' table and the 'customer' table
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL, c.C_ADDRESS, c.C_PHONE, c.C_COMMENT, n.N_NAME
        FROM customer c
        JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY
    """)
    customers_nations = cursor.fetchall()
    df_mysql = pd.DataFrame(customers_nations, columns=['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT', 'N_NAME'])

mysql_conn.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query the Redis 'orders' table
df_orders = redis_client.get('orders')
df_orders = pd.read_json(df_orders)

# Filtering orders between the specific date range
df_orders = df_orders[
    (df_orders['O_ORDERDATE'] >= '1993-10-01') &
    (df_orders['O_ORDERDATE'] <= '1993-12-31')
]

# Query the Redis 'lineitem' table
df_lineitem = redis_client.get('lineitem')
df_lineitem = pd.read_json(df_lineitem)

# Filtering lineitems with a return flag 'R'
df_lineitem = df_lineitem[df_lineitem['L_RETURNFLAG'] == 'R']

# Merging Redis tables 'df_orders' with 'df_lineitem' and calculating 'REVENUE'
df_orders_lineitem = pd.merge(df_orders, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df_orders_lineitem['REVENUE'] = df_orders_lineitem['L_EXTENDEDPRICE'] * (1 - df_orders_lineitem['L_DISCOUNT'])

# Grouping the data by 'O_CUSTKEY' and summarizing revenue
df_revenue = df_orders_lineitem.groupby('O_CUSTKEY')['REVENUE'].sum().reset_index()

# Merging the MySQL and Redis DataFrames
df_final_results = pd.merge(df_mysql, df_revenue, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Selecting necessary columns and sorting as per query requirements
df_final_results = df_final_results.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
                                                ascending=[True, True, True, False])
df_final_results = df_final_results[['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]

# Saving results to CSV
df_final_results.to_csv('query_output.csv', index=False)
