import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379)

# Query customers and orders from MySQL
customer_query = "SELECT C_CUSTKEY, C_NAME FROM customer;"
orders_query = "SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE FROM orders WHERE O_ORDERKEY IN (SELECT L_ORDERKEY FROM lineitem GROUP BY L_ORDERKEY HAVING SUM(L_QUANTITY) > 300);"

# Load data into pandas dataframes
with mysql_conn.cursor() as cursor:
    cursor.execute(customer_query)
    customers = pd.DataFrame(cursor.fetchall(), columns=['C_CUSTKEY', 'C_NAME'])

    cursor.execute(orders_query)
    orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

# Close MySQL connection
mysql_conn.close()

# Load lineitem data from Redis
lineitem_df = pd.DataFrame(redis_conn.get('lineitem'))

# Merge orders with lineitems on order key
orders_lineitems = orders.merge(lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by order key and calculate sum of quantities
order_quantities = orders_lineitems.groupby('O_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
order_quantities.rename(columns={'L_QUANTITY': 'TOTAL_QUANTITY'}, inplace=True)

# Filter orders with total quantity > 300
orders_filtered = orders_lineitems[orders_lineitems['O_ORDERKEY'].isin(order_quantities[order_quantities['TOTAL_QUANTITY'] > 300]['O_ORDERKEY'])]

# Select relevant columns and merge with customers
result = orders_filtered[['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_TOTALPRICE']].merge(customers, on='C_CUSTKEY')

# Add total quantity column
result = result.merge(order_quantities, on='O_ORDERKEY')

# Select and sort final output
final_result = result[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY']]
final_result = final_result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write result to CSV
final_result.to_csv('query_output.csv', index=False)
