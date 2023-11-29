# query_code.py
import pandas as pd
import pymysql
import direct_redis

# Connect to MySQL
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = conn.cursor()

# The relevant SQL query part to fetch data from MySQL's customer table
sql = """
SELECT
    LEFT(C_PHONE, 2) AS CNTRYCODE,
    COUNT(*) as NUM_CUSTOMERS,
    SUM(C_ACCTBAL) as TOTAL_ACCTBAL
FROM
    customer
WHERE
    LEFT(C_PHONE, 2) IN ('20', '40', '22', '30', '39', '42', '21')
    AND C_ACCTBAL > (
        SELECT AVG(C_ACCTBAL)
        FROM customer
        WHERE C_ACCTBAL > 0
    )
GROUP BY
    CNTRYCODE
HAVING
    NUM_CUSTOMERS > 0
ORDER BY
    CNTRYCODE;
"""

cursor.execute(sql)
mysql_result = cursor.fetchall()

# Create a DataFrame for MySQL result
df_mysql = pd.DataFrame(mysql_result, columns=['CNTRYCODE', 'NUM_CUSTOMERS', 'TOTAL_ACCTBAL'])

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch Orders DataFrame from Redis
df_orders = redis_conn.get('orders')
df_orders = pd.read_msgpack(df_orders)

# Filter orders that are older than 7 years
df_orders['O_ORDERDATE'] = pd.to_datetime(df_orders['O_ORDERDATE'])
seven_years_ago = pd.to_datetime("now") - pd.DateOffset(years=7)
df_orders_old = df_orders[df_orders['O_ORDERDATE'] < seven_years_ago]

# Get the customer keys of the customers who have not placed order for 7 years
custkeys_no_orders_7_years = set(df_mysql['C_CUSTKEY']) - set(df_orders_old['O_CUSTKEY'])

# Filter customers who have not placed orders for 7 years
df_customers_no_orders = df_mysql[df_mysql['C_CUSTKEY'].isin(custkeys_no_orders_7_years)]

# Calculate the results
result = df_customers_no_orders.groupby('CNTRYCODE').agg(
    NUM_CUSTOMERS=('C_CUSTKEY', 'count'),
    TOTAL_ACCTBAL=('C_ACCTBAL', 'sum')
).reset_index()

# Sort the result
result.sort_values(by='CNTRYCODE', inplace=True)

# Write the results to a csv file
result.to_csv('query_output.csv', index=False)

# Close the database connection
cursor.close()
conn.close()
