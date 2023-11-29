import pymysql
import direct_redis
import pandas as pd
from datetime import datetime, timedelta

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Getting current date to calculate 7 years ago
current_date = datetime.now()
seven_years_ago = current_date - timedelta(days=7*365)

# MySQL query
mysql_query = """
SELECT O_CUSTKEY
FROM orders
WHERE O_ORDERDATE <= '{}';
""".format(seven_years_ago.strftime('%Y-%m-%d'))

# Execute the query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    # Fetch all customers who have placed orders within 7 years
    customers_with_orders = set(row[0] for row in cursor.fetchall())

# Close the connection
mysql_conn.close()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Extract customer data from Redis and convert to Pandas DataFrame
customer_data = pd.DataFrame(redis_conn.get('customer'))

# Filter customer data according to the conditions
country_codes = ('20', '40', '22', '30', '39', '42', '21')
filtered_customers = customer_data[
    (customer_data['C_CUSTKEY'].apply(lambda x: x not in customers_with_orders)) &  # no orders in the last 7 years
    (customer_data['C_PHONE'].str[:2].isin(country_codes)) &  # one of the specified country codes
    (customer_data['C_ACCTBAL'] > 0.00)  # account balance larger than 0.00
]

# Calculate average account balance for those countries
average_balance = filtered_customers['C_ACCTBAL'].mean()

# Further filter according to account balance above average
final_customers = filtered_customers[filtered_customers['C_ACCTBAL'] > average_balance]

# Group by the first two characters of C_PHONE and perform the necessary calculations
output = final_customers.groupby(filtered_customers['C_PHONE'].str[:2]).agg({
    'C_CUSTKEY': 'count',
    'C_ACCTBAL': 'sum'
}).reset_index()

output.columns = ['CNTRYCODE', 'NUM_CUSTOMERS', 'TOTAL_ACCTBAL']
output = output.sort_values('CNTRYCODE').reset_index(drop=True)

# Write to CSV
output.to_csv('query_output.csv', index=False)
