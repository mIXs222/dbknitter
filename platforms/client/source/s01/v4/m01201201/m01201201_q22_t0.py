import pymysql
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
def connect_mysql():
    return pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Redis connection
def connect_redis():
    return DirectRedis(host='redis', port=6379, db=0)

# Fetching customer data from Redis and loading as DataFrame
r = connect_redis()
customer_data = r.get('customer')
customer_df = pd.read_json(customer_data, orient='records')

# Applying filters to the customer DataFrame
filtered_customer_df = customer_df[
    customer_df['C_PHONE'].str[:2].isin(['20', '40', '22', '30', '39', '42', '21']) &
    (customer_df['C_ACCTBAL'] > 0.00)
]
avg_acctbal = filtered_customer_df['C_ACCTBAL'].mean()
rich_customers_df = filtered_customer_df[filtered_customer_df['C_ACCTBAL'] > avg_acctbal]

# Fetching orders data from MySQL
cnx = connect_mysql()
with cnx.cursor() as cursor:
    cursor.execute("SELECT DISTINCT O_CUSTKEY FROM orders")
    orders_data = cursor.fetchall()
orders_customers = pd.DataFrame(orders_data, columns=['O_CUSTKEY'])

cnx.close()

# Finding customers not having orders
rich_customers_set = set(rich_customers_df['C_CUSTKEY'])
orders_customers_set = set(orders_customers['O_CUSTKEY'])
final_customers_keys = list(rich_customers_set - orders_customers_set)
final_customers_df = rich_customers_df[rich_customers_df['C_CUSTKEY'].isin(final_customers_keys)]

# Creating subgroup with the country code and account balance
desired_columns = final_customers_df[['C_PHONE', 'C_ACCTBAL']].copy()
desired_columns['CNTRYCODE'] = desired_columns['C_PHONE'].str[:2]
custsale = desired_columns.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_PHONE', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index()

# Sorting by country code
custsale = custsale.sort_values('CNTRYCODE')

# Writing final result to CSV
custsale.to_csv('query_output.csv', index=False)
