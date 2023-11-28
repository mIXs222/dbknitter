import pymongo
from bson.son import SON
import pandas as pd
import direct_redis

# MongoDB connection and aggregation
client = pymongo.MongoClient(host="mongodb", port=27017)
db = client['tpch']

pipeline = [
    {
        "$group": {
            "_id": "$L_ORDERKEY",
            "total_quantity": {"$sum": "$L_QUANTITY"}
        }
    },
    {
        "$match": {
            "total_quantity": {"$gt": 300}
        }
    }
]
order_keys_with_total_gt_300 = list(db.lineitem.aggregate(pipeline))
order_keys = [doc['_id'] for doc in order_keys_with_total_gt_300]

# get relevant customer data
customer_data = pd.DataFrame(list(db.customer.find({}, {
    "_id": 0,
    "C_CUSTKEY": 1,
    "C_NAME": 1
})))

# Redis connection
redis_client = direct_redis.DirectRedis(host="redis", port=6379)
orders_df = pd.read_json(redis_client.get('orders'))
# Pre-filter orders that match the order_keys from MongoDB
filtered_orders = orders_df[orders_df["O_ORDERKEY"].isin(order_keys)]

# Combine data
combined_data = filtered_orders.merge(customer_data, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Aggregate total quantity for each order from MongoDB data
order_quantity = pd.DataFrame(order_keys_with_total_gt_300)
combined_data = combined_data.merge(order_quantity, left_on='O_ORDERKEY', right_on='_id')

# Select necessary fields
result = combined_data[["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE", "total_quantity"]]
result = result.sort_values(by=["O_TOTALPRICE", "O_ORDERDATE"], ascending=[False, True])

# Saving the result to a CSV file
result.to_csv('query_output.csv', index=False)
