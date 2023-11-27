import pandas as pd
import pymysql
import direct_redis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get average account balance from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute(
        """
        SELECT AVG(C_ACCTBAL)
        FROM customer
        WHERE C_ACCTBAL > 0.00
        AND SUBSTR(C_PHONE , 1 , 2) IN ('20', '40', '22', '30', '39', '42', '21')
        """
    )
    avg_acctbal = cursor.fetchone()[0]

# Get customer data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute(
        """
        SELECT SUBSTR(C_PHONE, 1 , 2) AS CNTRYCODE, C_CUSTKEY, C_ACCTBAL
        FROM customer
        WHERE SUBSTR(C_PHONE , 1 , 2) IN ('20', '40', '22', '30', '39', '42', '21')
        AND C_ACCTBAL > %s
        """, (avg_acctbal,)
    )
    customers = cursor.fetchall()

# Get orders data from Redis
orders_df = pd.read_json(redis_conn.get('orders'), orient='records')

# Filter out customers with orders
cust_keys_with_orders = orders_df['O_CUSTKEY'].unique()
customers_filtered = [
    (cntrycode, acctbal) for cntrycode, custkey, acctbal in customers
    if custkey not in cust_keys_with_orders
]

# Create DataFrame
res_df = pd.DataFrame(customers_filtered, columns=['CNTRYCODE', 'C_ACCTBAL']).groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='CNTRYCODE', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index()

# Sort by CNTRYCODE
res_df.sort_values(by='CNTRYCODE', inplace=True)

# Write to query_output.csv
res_df.to_csv('query_output.csv', index=False)

# Closing connections
mysql_conn.close()
