import pymysql
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Query to fetch average account balance from MySQL
avg_acctbal_query = """
SELECT AVG(C_ACCTBAL) AS avg_balance FROM customer
WHERE C_ACCTBAL > 0.00 AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21');
"""

# Execute the average account balance query
with mysql_connection.cursor() as cursor:
    cursor.execute(avg_acctbal_query)
    result = cursor.fetchone()
    avg_acctbal = result[0]

# Query to fetch customers from MySQL
customer_query = f"""
SELECT C_CUSTKEY, SUBSTR(C_PHONE, 1 , 2) AS CNTRYCODE, C_ACCTBAL
FROM customer
WHERE C_ACCTBAL > {avg_acctbal}
AND SUBSTR(C_PHONE , 1 , 2) IN
('20', '40', '22', '30', '39', '42', '21')
"""

# Execute the customer query and fetch data
with mysql_connection.cursor() as cursor:
    cursor.execute(customer_query)
    customers = cursor.fetchall()

df_customers = pd.DataFrame(customers, columns=['C_CUSTKEY', 'CNTRYCODE', 'C_ACCTBAL'])

# Fetch orders from Redis and convert to DataFrame
orders_str = redis_connection.get('orders')
orders_df = pd.read_json(orders_str, orient='table')

# Filter out customers with orders
cust_with_orders = orders_df['O_CUSTKEY'].unique()
df_customers = df_customers[~df_customers['C_CUSTKEY'].isin(cust_with_orders)]

# Aggregate the results
output_df = df_customers.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index()

# Write results to CSV
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mysql_connection.close()
redis_connection.close()
