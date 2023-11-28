import pandas as pd
import pymysql
import direct_redis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch customers with positive account balances from MySQL
mysql_cursor.execute("""
    SELECT C_CUSTKEY, C_NAME, C_PHONE, C_ACCTBAL, SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE
    FROM customer
    WHERE C_ACCTBAL > 0
""")
positive_customers = pd.DataFrame(mysql_cursor.fetchall(),
                                  columns=['C_CUSTKEY', 'C_NAME', 'C_PHONE', 'C_ACCTBAL', 'CNTRYCODE'])

# Get average account balance for specified country codes from MySQL
avg_balances = positive_customers.loc[
    positive_customers['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21'])
].groupby('CNTRYCODE')['C_ACCTBAL'].mean().reset_index()

# Get orders from Redis
orders = pd.DataFrame(redis_conn.get('orders'))

# Filter customers that never made an order
customer_orders = pd.merge(
    positive_customers,
    orders[['O_CUSTKEY']],
    left_on='C_CUSTKEY',
    right_on='O_CUSTKEY',
    how='left',
    indicator=True
)

filter_customers = customer_orders[customer_orders['_merge'] == 'left_only']

# Filter customers with account balance greater than average in specified country
filter_customers = filter_customers[
    (filter_customers['CNTRYCODE'].isin(avg_balances['CNTRYCODE'])) &
    (filter_customers['C_ACCTBAL'] > filter_customers['CNTRYCODE'].map(avg_balances.set_index('CNTRYCODE')['C_ACCTBAL']))
]

# Aggregate data
results = filter_customers.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index().sort_values('CNTRYCODE')

# Write to CSV
results.to_csv('query_output.csv', index=False)

# Cleanup
mysql_conn.close()
