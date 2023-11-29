import pymysql
import pandas as pd
from datetime import datetime
import direct_redis

# Establish connection to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')
try:
    # MySQL query
    asia_nations_query = """
    SELECT n.N_NAME, r.R_NAME
    FROM nation n
    JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
    WHERE r.R_NAME = 'ASIA'
    """
    asia_nations = pd.read_sql(asia_nations_query, mysql_connection)
    
    # Get the nation keys for ASIA region
    asia_nation_keys = asia_nations['N_NATIONKEY'].tolist()
finally:
    mysql_connection.close()

# Establish connection to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve Redis dataframes
customers_df = pd.DataFrame(eval(redis_connection.get('customer')))
orders_df = pd.DataFrame(eval(redis_connection.get('orders')))
lineitem_df = pd.DataFrame(eval(redis_connection.get('lineitem')))

# Filter customers and suppliers by nation keys, orders by date and join them with lineitems
customers_df = customers_df[customers_df['C_NATIONKEY'].isin(asia_nation_keys)]
orders_df = orders_df[(orders_df['O_ORDERDATE'] >= datetime(1990, 1, 1)) & 
                      (orders_df['O_ORDERDATE'] <= datetime(1995, 1, 1))]
lineitem_df = lineitem_df[lineitem_df['L_SUPPKEY'].isin(customers_df['C_CUSTKEY'].tolist())]

# Combine the dataframes to gather the required information
df_combined = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df_combined = df_combined.merge(customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Calculate the revenue
df_combined['REVENUE'] = df_combined['L_EXTENDEDPRICE'] * (1 - df_combined['L_DISCOUNT'])
df_combined = df_combined.groupby('C_NATIONKEY')['REVENUE'].sum().reset_index()

# Join with the nation data to get nation names
final_result = df_combined.merge(asia_nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select and sort the columns as required
final_result = final_result[['N_NAME', 'REVENUE']].sort_values(by='REVENUE', ascending=False)

# Write the final result to a CSV file
final_result.to_csv('query_output.csv', index=False)
