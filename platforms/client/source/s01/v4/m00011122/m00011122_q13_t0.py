# customers_orders_join.py

import pymongo
import redis
import pandas as pd

# MongoDB connection
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]
customer_collection = mongodb_db["customer"]

# Retrieve data from MongoDB
customer_data = pd.DataFrame(list(customer_collection.find({}, {
    "_id": 0,
    "C_CUSTKEY": 1,
    "C_NAME": 1
})))

# Redis connection (assuming DirectRedis is a provided class similar to redis.Redis)
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
orders_keys = redis_client.keys('orders:*')

# Retrieve data from Redis
orders_data = pd.DataFrame(columns=["O_ORDERKEY", "O_CUSTKEY", "O_COMMENT"])
for key in orders_keys:
    order = redis_client.hgetall(key)
    if order:  # to ensure no empty results are appended
        orders_data = orders_data.append({
            "O_ORDERKEY": order.get("O_ORDERKEY"),
            "O_CUSTKEY": order.get("O_CUSTKEY"),
            "O_COMMENT": order.get("O_COMMENT")
        }, ignore_index=True)

# Convert O_CUSTKEY to numeric for proper join
orders_data['O_CUSTKEY'] = pd.to_numeric(orders_data['O_CUSTKEY'])

# Perform a LEFT OUTER JOIN
merged_data = customer_data.merge(orders_data, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Filter out rows with 'pending%deposits%' in O_COMMENT
merged_data = merged_data[~merged_data['O_COMMENT'].str.contains('pending%deposits%', na=True)]

# Group by C_CUSTKEY to count O_ORDERKEY
grouped = merged_data.groupby('C_CUSTKEY', as_index=False).agg(C_COUNT=('O_ORDERKEY', 'count'))

# Count the number of customers per C_COUNT
custdist = grouped.groupby('C_COUNT', as_index=False).agg(CUSTDIST=('C_COUNT', 'count'))

# Sort by CUSTDIST DESC, C_COUNT DESC
custdist = custdist.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write output to CSV
custdist.to_csv('query_output.csv', index=False)
