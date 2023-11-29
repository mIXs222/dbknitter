# file: execute_query.py

import pymysql
import direct_redis
import pandas as pd

# Connection information for MySQL
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}
# Connect to MySQL
mysql_connection = pymysql.connect(**mysql_conn_info)
cursor = mysql_connection.cursor()

# Retrieve data from MySQL tpch database
query_nation = "SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME IN ('INDIA', 'JAPAN')"
cursor.execute(query_nation)
nations = cursor.fetchall()
nation_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])
nation_df.set_index('N_NAME', inplace=True)

# Connection information for Redis
redis_conn_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0,
}
# Connect to Redis
redis_connection = direct_redis.DirectRedis(**redis_conn_info)

# Retrieve data from Redis with the specified keys
supplier_df = pd.DataFrame(eval(redis_connection.get('supplier')))
customer_df = pd.DataFrame(eval(redis_connection.get('customer')))
lineitem_df = pd.DataFrame(eval(redis_connection.get('lineitem')))
# Filtering lineitems from 1995 and 1996
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1995-01-01') &
                          (lineitem_df['L_SHIPDATE'] <= '1996-12-31')]

# Join data
result = lineitem_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
result = result.merge(customer_df, left_on='L_ORDERKEY', right_on='C_CUSTKEY')
result = result.merge(nation_df, left_on='S_NATIONKEY', right_index=True, how='inner')
result = result.rename(columns={'N_NAME': 'SUPP_NATION'})
result = result.merge(nation_df, left_on='C_NATIONKEY', right_index=True, how='inner')
result = result.rename(columns={'N_NAME': 'CUST_NATION'})

# Calculate revenue and filter nations
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])
result['L_YEAR'] = pd.to_datetime(result['L_SHIPDATE']).dt.year
result = result[(result['SUPP_NATION'] != result['CUST_NATION']) & 
                ((result['SUPP_NATION'].isin(['INDIA', 'JAPAN'])) &
                 (result['CUST_NATION'].isin(['INDIA', 'JAPAN'])))]

# Group, sum, and sort results
grouped_result = result.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR']).agg({'REVENUE': 'sum'}).reset_index()
grouped_result = grouped_result.sort_values(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Output the columns in the specified order
output_df = grouped_result[['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']]
output_df.to_csv('query_output.csv', index=False)

# Remember to close the connections
cursor.close()
mysql_connection.close()
redis_connection.close()
