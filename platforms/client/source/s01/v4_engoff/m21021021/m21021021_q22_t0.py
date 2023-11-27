# query.py
import pymysql
import pandas as pd
import direct_redis

# Establish MySQL connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Define the list of country codes
country_codes = ['20', '40', '22', '30', '39', '42', '21']
seven_years_ago = pd.Timestamp.now() - pd.DateOffset(years=7)

# Fetch customers data from MySQL
with mysql_connection.cursor() as cursor:
    cursor.execute("""
        SELECT C_CUSTKEY, C_PHONE, C_ACCTBAL
        FROM customer
        WHERE LEFT(C_PHONE, 2) IN ('20', '40', '22', '30', '39', '42', '21')
        AND C_ACCTBAL > 0.00
    """)
    customers_data = cursor.fetchall()

# Convert to DataFrame
customers_df = pd.DataFrame(customers_data, columns=['C_CUSTKEY', 'C_PHONE', 'C_ACCTBAL'])

# Filter customers with account balance greater than 0
filtered_customers_df = customers_df[customers_df['C_ACCTBAL'] > 0]

# Setup Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get orders data from Redis
orders_data = r.get('orders')
orders_df = pd.read_json(orders_data)

# Filter out orders within the last 7 years
filtered_orders_df = orders_df[orders_df['O_ORDERDATE'] <= seven_years_ago.strftime('%Y-%m-%d')]

# Identify customers who have NOT placed orders for 7 years
customers_no_recent_orders = set(filtered_customers_df['C_CUSTKEY']).difference(set(filtered_orders_df['O_CUSTKEY']))

# Filter customer DataFrame for those customers
output_df = filtered_customers_df[filtered_customers_df['C_CUSTKEY'].isin(customers_no_recent_orders)]

# Summarize the result
result = output_df.groupby(output_df['C_PHONE'].str[:2]).agg({
    'C_CUSTKEY': 'count',
    'C_ACCTBAL': 'mean'
}).rename(columns={'C_CUSTKEY': 'CustomerCount', 'C_ACCTBAL': 'AverageAccountBalance'})

# Write the result to a CSV file
result.to_csv('query_output.csv')

# Close connections
mysql_connection.close()
