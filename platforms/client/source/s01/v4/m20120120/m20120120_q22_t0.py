# query.py
import pymongo
from bson import regex
import pandas as pd
import direct_redis
import numpy as np

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client['tpch']
customers_coll = db['customer']

# Perform aggregations for MongoDB subquery
pipeline = [
    {"$match": {
        "C_PHONE": {"$regex": regex.Regex(r'^(20|40|22|30|39|42|21)')},
        "C_ACCTBAL": {"$gt": 0.0}
    }},
    {"$group": {
        "_id": None,
        "avg_balance": {"$avg": "$C_ACCTBAL"}
    }}
]
average_result = list(customers_coll.aggregate(pipeline))
average_balance = average_result[0]['avg_balance'] if average_result else 0

# Apply filter based on subquery result
customers_data = list(customers_coll.find({
    "C_PHONE": {"$regex": regex.Regex(r'^(20|40|22|30|39|42|21)')},
    "C_ACCTBAL": {"$gt": average_balance}
}, {
    "_id": 0,
    "C_CUSTKEY": 1,
    "C_PHONE": 1,
    "C_ACCTBAL": 1
}))

# Convert to DataFrame
customers_df = pd.DataFrame(customers_data)
customers_df['CNTRYCODE'] = customers_df['C_PHONE'].str[:2]

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
orders_keys = r.keys('orders:*')

# Load orders data
orders = []
for key in orders_keys:
    order = r.get(key)
    if order:
        orders.append(pd.read_json(order, typ='series'))

# If there are any orders, create a DataFrame
orders_df = pd.DataFrame(orders) if orders else pd.DataFrame(columns=['O_CUSTKEY'])

# Filter customers who do not have orders
no_order_customers = customers_df[~customers_df['C_CUSTKEY'].isin(orders_df['O_CUSTKEY'])]

# Perform final aggregation and sort by CNTRYCODE
result_df = no_order_customers.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum'),
).reset_index()

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
