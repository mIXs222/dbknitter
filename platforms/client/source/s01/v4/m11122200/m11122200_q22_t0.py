# imports
import pymysql
import pandas as pd
import direct_redis

# Database connections
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

redis_host = 'redis'
redis_port = 6379

# Redis connection
redis_conn = direct_redis.DirectRedis(host=redis_host, port=redis_port)

# MySQL Query
mysql_query = """
SELECT O_CUSTKEY FROM orders
"""
# Execute MySQL Query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    orders_data = cursor.fetchall()

# Fetch data from Redis
customer_df = redis_conn.get('customer')
customer_df = pd.read_json(customer_df)

# Convert orders data to DataFrame
orders_custkey = pd.DataFrame(orders_data, columns=['O_CUSTKEY'])

# Process customer DataFrame
customer_df = customer_df[~customer_df['C_CUSTKEY'].isin(orders_custkey['O_CUSTKEY'])]
customer_df['CNTRYCODE'] = customer_df['C_PHONE'].str[:2]

# Filter based on the country code
filtered_customer_df = customer_df[customer_df['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21'])]

# Compute average account balance
avg_acctbal = filtered_customer_df[filtered_customer_df['C_ACCTBAL'] > 0]['C_ACCTBAL'].mean()

# Filter customers with account balance greater than the average
final_customer_df = filtered_customer_df[filtered_customer_df['C_ACCTBAL'] > avg_acctbal]

# Group by CNTRYCODE and calculate the aggregates
result_df = final_customer_df.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index()

# Sort the DataFrame by country code
result_df = result_df.sort_values('CNTRYCODE')

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

# Close the database connections
mysql_conn.close()
redis_conn.close()
