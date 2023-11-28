import pymongo
import pandas as pd
import csv

# Establish a connection to MongoDB
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]
mongo_customer_table = mongodb_db["customer"]

# Read data from MongoDB
customers_df = pd.DataFrame(list(mongo_customer_table.find()))

# Establish a connection to Redis
import direct_redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read data from Redis
orders_bytes = redis_client.get('orders')
orders_str = orders_bytes.decode("utf-8")
orders_data = [row.split(',') for row in orders_str.strip().split('\n')]
orders_columns = ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 
                  'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']
orders_df = pd.DataFrame(orders_data, columns=orders_columns)

# Convert O_CUSTKEY to numeric to match types between dataframes for the merge
orders_df['O_CUSTKEY'] = pd.to_numeric(orders_df['O_CUSTKEY'])

# Exclude orders with comments containing the phrases 'pending' and 'deposits.'
excluded_orders_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits')]

# Perform the LEFT JOIN between customers and orders on customer key (C_CUSTKEY & O_CUSTKEY)
merged_df = pd.merge(customers_df, excluded_orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Create a subquery (C_ORDERS) DataFrame consisting of the count of orders for each customer
c_orders = merged_df.groupby('C_CUSTKEY').agg(C_COUNT=('O_ORDERKEY', 'count')).reset_index()

# Group by C_COUNT (count of orders per customer), calculate number of customers (CUSTDIST) for each count
distribution_df = c_orders.groupby('C_COUNT').agg(CUSTDIST=('C_CUSTKEY', 'count')).reset_index()

# Sort the results by CUSTDIST (descending) and C_COUNT (also in descending order)
sorted_distribution_df = distribution_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write the sorted output to a CSV file
sorted_distribution_df.to_csv('query_output.csv', index=False)

# Clean up clients
mongodb_client.close()
redis_client.close()
