import pandas as pd
import pymysql
from sqlalchemy import create_engine
import direct_redis

# Connect to MySQL
mysql_connection_info = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch',
}
mysql_engine = create_engine(f"mysql+pymysql://{mysql_connection_info['user']}:{mysql_connection_info['password']}@{mysql_connection_info['host']}/{mysql_connection_info['database']}")

# Connect to Redis
redis_hostname = 'redis'
redis_port = 6379
redis_db = direct_redis.DirectRedis(host=redis_hostname, port=redis_port, db=0)

# Query MySQL for orders not existing
query_orders = """SELECT DISTINCT O_CUSTKEY FROM orders"""
df_orders = pd.read_sql_query(query_orders, mysql_engine)

# Get customer dataframe from Redis
df_customer = redis_db.get('customer')
df_customer = pd.read_json(df_customer)

# Extract country code from phone numbers
df_customer['CNTRYCODE'] = df_customer['C_PHONE'].str[:2]

# Include customers with positive account balances greater than the average
positive_balances = df_customer[df_customer['C_ACCTBAL'] > 0]
average_balances = positive_balances.groupby('CNTRYCODE')['C_ACCTBAL'].mean().reset_index()
average_balances.columns = ['CNTRYCODE', 'AVG_ACCTBAL']

# Filter customers based on the country codes and account balances
selected_country_codes = ['20', '40', '22', '30', '39', '42', '21']
filtered_customers = df_customer.merge(average_balances, on='CNTRYCODE')
filtered_customers = filtered_customers[
    (filtered_customers['C_ACCTBAL'] > filtered_customers['AVG_ACCTBAL']) &
    (filtered_customers['CNTRYCODE'].isin(selected_country_codes))
]

# Exclude customers who have placed orders
filtered_customers = filtered_customers[~filtered_customers['C_CUSTKEY'].isin(df_orders['O_CUSTKEY'])]

# Aggregate result
result = filtered_customers.groupby('CNTRYCODE').agg({'C_CUSTKEY': 'count', 'C_ACCTBAL': 'sum'}).reset_index()
result.columns = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']

# Sort and write to csv
result.sort_values('CNTRYCODE', ascending=True).to_csv('query_output.csv', index=False)
