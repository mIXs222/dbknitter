# query.py

from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd
from decimal import Decimal
import csv

# Connect to MongoDB
mongo_client = MongoClient(host="mongodb", port=27017)
mongo_db = mongo_client["tpch"]
orders_col = mongo_db["orders"]

# Retrieve orders which have not been shipped as of 1995-03-15
unshipped_orders = list(orders_col.find({
    "O_ORDERDATE": {"$lt": "1995-03-15"},
}, {
    "O_ORDERKEY": 1,
    "O_SHIPPRIORITY": 1,
    "_id": 0
}))

# Connect to Redis and retrieve customer and lineitem data
redis_client = DirectRedis(host="redis", port=6379, db=0)
customers = pd.read_json(redis_client.get('customer'))
lineitems = pd.read_json(redis_client.get('lineitem'))

# Filter customers where market segment is BUILDING
building_customers = customers[customers['C_MKTSEGMENT'] == 'BUILDING']

# Prepare the dataframe from the unshipped orders
orders_df = pd.DataFrame(unshipped_orders)

# Merge unshipped orders with building customers
merged_orders = orders_df.merge(building_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Calculate potential revenue for lineitems
lineitems['POTENTIAL_REVENUE'] = lineitems['L_EXTENDEDPRICE'] * (1 - lineitems['L_DISCOUNT'])

# Merge the merged orders with lineitems to calculate their potential revenue
final_result = merged_orders.merge(lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by order key and sum potential revenue, sort by revenue
grouped_result = final_result.groupby(['O_ORDERKEY', 'O_SHIPPRIORITY']).agg(
    POTENTIAL_REVENUE=('POTENTIAL_REVENUE', sum)
).reset_index().sort_values('POTENTIAL_REVENUE', ascending=False)

# Write the final result to a csv file
grouped_result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
