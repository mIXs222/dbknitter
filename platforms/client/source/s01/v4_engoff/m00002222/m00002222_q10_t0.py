import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Fetch nation table from MySQL
query_nation = "SELECT N_NATIONKEY, N_NAME FROM nation;"
nation_df = pd.read_sql(query_nation, mysql_conn)

# Close MySQL Connection
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch Redis tables as DataFrames
customer_df = redis_conn.get('customer')
orders_df = redis_conn.get('orders')
lineitem_df = redis_conn.get('lineitem')

# Filtering for specified quarter
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
filtered_orders = orders_df[(orders_df['O_ORDERDATE'] >= '1993-10-01') &
                            (orders_df['O_ORDERDATE'] <= '1994-01-01')]

# Joining DataFrames
result_df = (filtered_orders.merge(lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R'],
                                   left_on='O_ORDERKEY',
                                   right_on='L_ORDERKEY', how='inner')
                             .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
                             .merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY'))

# Calculate lost revenue
result_df['LOST_REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Grouping and sorting results
output_df = result_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 
                               'C_ACCTBAL', 'C_COMMENT'])['LOST_REVENUE'] \
                     .sum().reset_index() \
                     .sort_values(by=['LOST_REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
                                  ascending=[False, True, True, True])

# Write result to CSV
output_df.to_csv('query_output.csv', index=False)
