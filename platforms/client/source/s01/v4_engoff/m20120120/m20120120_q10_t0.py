# importing necessary libraries
import pymysql
import pymongo
import pandas as pd
import csv
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client['tpch']

# Connect to Redis using DirectRedis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# MySQL Query
mysql_query = """
SELECT 
    c.C_NAME, c.C_ADDRESS, n.N_NAME, c.C_PHONE, c.C_ACCTBAL, c.C_COMMENT,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue_lost
FROM
    lineitem l
JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY
WHERE
    l.L_RETURNFLAG = 'R'
AND o.O_ORDERDATE >= '1993-10-01'
AND o.O_ORDERDATE < '1994-01-01'
GROUP BY c.C_CUSTKEY
ORDER BY revenue_lost DESC, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL;
"""

with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# Redis Query (since Redis doesn't support SQL-like query, we need to fetch and process in pandas)
redis_nation_df = pd.read_csv(redis_connection.get('nation').decode('utf-8'))
redis_orders_df = pd.read_csv(redis_connection.get('orders').decode('utf-8'))

# MongoDB Query
mongodb_results = list(mongodb.customer.find())

# Merge MongoDB results with Redis results
merged_customers = pd.DataFrame(mongodb_results)
orders_df = pd.DataFrame(redis_orders_df)
nation_df = pd.DataFrame(redis_nation_df)

# Merge orders with customers data
merged_customer_orders = pd.merge(orders_df, merged_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
# Merge the result with nation data
final_df = pd.merge(merged_customer_orders, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Convert the revenue_lost into a summable format
final_df['revenue_lost'] = final_df.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)

# Apply the time constraint for orders
final_df = final_df[(final_df['O_ORDERDATE'] >= datetime(1993, 10, 1)) & (final_df['O_ORDERDATE'] < datetime(1994, 1, 1))]

# Group by customer info and sum the revenue lost, and then sort by the specified fields
result_df = final_df.groupby(['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT'])['revenue_lost'].sum().reset_index().sort_values(by=['revenue_lost', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True])

# Save the result to a CSV file
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_connection.close()
mongo_client.close()
redis_connection.close()
