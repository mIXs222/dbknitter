# query.py
import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
# Create pandas DataFrame from orders using the connection
orders_sql = "SELECT O_CUSTKEY FROM orders WHERE YEAR(O_ORDERDATE) <= YEAR(CURRENT_DATE) - 7"
orders_df = pd.read_sql(orders_sql, mysql_connection)
mysql_connection.close()

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379)
# Get customer data from redis and create a pandas DataFrame
customer_df = pd.read_json(r.get('customer'), orient='records')

# Filter customers from countries with specified codes
filtered_customers = customer_df[customer_df['C_PHONE'].str[:2].isin(['20', '40', '22', '30', '39', '42', '21'])]

# Filter customers with positive account balance
positive_bal_customers = filtered_customers[filtered_customers['C_ACCTBAL'] > 0.00]
avg_bal_by_country = positive_bal_customers.groupby(positive_bal_customers['C_PHONE'].str[:2])['C_ACCTBAL'].mean().reset_index()
avg_bal_by_country.columns = ['CNTRYCODE', 'AVG_ACCTBAL']

# Find customers with balance greater than average and not in orders
filtered_customers = pd.merge(filtered_customers, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left', indicator=True)
customers_without_orders = filtered_customers[filtered_customers['_merge'] == 'left_only']
eligible_customers = customers_without_orders.merge(avg_bal_by_country, left_on=customers_without_orders['C_PHONE'].str[:2], right_on='CNTRYCODE')
eligible_customers = eligible_customers[eligible_customers['C_ACCTBAL'] > eligible_customers['AVG_ACCTBAL']]

# Aggregating data by country code
result = eligible_customers.groupby('CNTRYCODE').agg({'C_CUSTKEY': 'count', 'C_ACCTBAL': 'sum'}).reset_index()
result.columns = ['CNTRYCODE', 'NUM_CUSTOMERS', 'TOTAL_ACCTBAL']
result = result.sort_values('CNTRYCODE')

# Save results to query_output.csv
result.to_csv('query_output.csv', index=False)
