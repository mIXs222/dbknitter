import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to connect to MongoDB and retrieve the customer data.
def get_customer_data():
    client = pymongo.MongoClient(host='mongodb', port=27017)
    db = client['tpch']
    customer_data = pd.DataFrame(list(db.customer.find({}, {'_id': 0})))
    client.close()
    return customer_data

# Function to connect to Redis and retrieve the orders and lineitem data.
def get_redis_data():
    r = DirectRedis(host='redis', port=6379, db=0)
    orders_data = pd.DataFrame(eval(r.get('orders')))
    lineitem_data = pd.DataFrame(eval(r.get('lineitem')))
    return orders_data, lineitem_data

# Get data from databases
customer = get_customer_data()
orders, lineitem = get_redis_data()

# Process lineitems to find the total quantity per order over 300
lineitem_sum_quantity = lineitem.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
lineitem_filtered = lineitem_sum_quantity[lineitem_sum_quantity['L_QUANTITY'] > 300]

# Merge the orders with the filtered lineitems to get the list of orders to consider
orders_filtered = orders.merge(lineitem_filtered, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Join the orders with customers
result = orders_filtered.merge(customer, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Calculate sum of quantities for each order and select the required fields
result = result.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({'L_QUANTITY':'sum'}).reset_index()

# Sort the results as required
result_sorted = result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write the results to a CSV file
result_sorted.to_csv('query_output.csv', index=False)
