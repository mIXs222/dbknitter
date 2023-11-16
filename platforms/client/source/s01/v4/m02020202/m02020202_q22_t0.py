import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Query MySQL for orders
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT O_CUSTKEY FROM orders;")
    orders = cursor.fetchall()
    mysql_df = pd.DataFrame(orders, columns=['O_CUSTKEY'])

# Close MySQL connection
mysql_connection.close()

# Connect to Redis and get the customer data
redis_connection = DirectRedis(host='redis', port=6379, db=0)
customer_data = redis_connection.get('customer')
customer_df = pd.read_json(customer_data)


# Filter customers based on the logic provided in the query
countries = ('20', '40', '22', '30', '39', '42', '21')
customer_df["CNTRYCODE"] = customer_df["C_PHONE"].str[:2]
filtered_customers = customer_df[
    (customer_df["CNTRYCODE"].isin(countries)) &
    (customer_df["C_ACCTBAL"] > 0)
]
avg_acctbal = filtered_customers["C_ACCTBAL"].mean()
filtered_customers = filtered_customers[
    (filtered_customers["C_ACCTBAL"] > avg_acctbal) &
    (~filtered_customers["C_CUSTKEY"].isin(mysql_df["O_CUSTKEY"]))
]

# Perform data aggregation
results = filtered_customers.groupby("CNTRYCODE").agg(
    NUMCUST=pd.NamedAgg(column="C_ACCTBAL", aggfunc="count"),
    TOTACCTBAL=pd.NamedAgg(column="C_ACCTBAL", aggfunc="sum")
).reset_index()

# Write output to CSV file
results.to_csv('query_output.csv', index=False)
