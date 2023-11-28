import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

with mysql_conn.cursor() as cursor:
    # Get the average account balances for customers with positive balances and specified country codes
    cursor.execute("""
        SELECT SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE, AVG(C_ACCTBAL)
        FROM customer
        WHERE C_ACCTBAL > 0 AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
        GROUP BY CNTRYCODE
    """)
    avg_balance_by_country = {row[0]: row[1] for row in cursor.fetchall()}

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve customer information from Redis
customer_df = pd.read_json(r.get('customer'), orient='records')

# Use only customer records with specified country codes
customer_df['CNTRYCODE'] = customer_df['C_PHONE'].str[:2]
customer_df = customer_df[customer_df['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21'])]

# Exclude customers with orders
customer_with_orders = pd.read_json(r.get('orders'), orient='records')
customer_with_orders_ids = customer_with_orders['O_CUSTKEY'].drop_duplicates()

customer_df = customer_df[~customer_df['C_CUSTKEY'].isin(customer_with_orders_ids)]

# Filter customers with account balance greater than the average in the country code
filtered_customers = customer_df[
    customer_df.apply(lambda x: x['C_ACCTBAL'] > avg_balance_by_country.get(x['CNTRYCODE'], 0), axis=1)
]

# Perform final aggregation and create output
custsale_df = (
    filtered_customers.groupby('CNTRYCODE')
    .agg(NUMCUST=('C_CUSTKEY', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum'))
    .sort_values('CNTRYCODE')
    .reset_index()
)

# Write the results to CSV
custsale_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
