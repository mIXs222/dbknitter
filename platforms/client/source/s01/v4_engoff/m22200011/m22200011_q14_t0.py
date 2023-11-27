# Import necessary libraries
import pymongo
from bson.json_util import dumps
import json
import pandas as pd
import direct_redis
from datetime import datetime

# MongoDB connection
mongoclient = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongoclient["tpch"]
lineitem_collection = mongodb["lineitem"]

# Retrieve lineitem data from MongoDB
query = {
    "L_SHIPDATE": {
        "$gte": datetime(1995, 9, 1),
        "$lt": datetime(1995, 10, 1)
    }
}
lineitem_cursor = lineitem_collection.find(query)
lineitem_list = list(lineitem_cursor)
lineitems = pd.DataFrame(lineitem_list)

# Calculate revenue
lineitems['revenue'] = lineitems['L_EXTENDEDPRICE'] * (1 - lineitems['L_DISCOUNT'])

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve part data from Redis
part_keys = r.keys('part:*')
part_data = []
for key in part_keys:
    part_data.append(json.loads(r.get(key)))

# Convert to DataFrame
parts = pd.DataFrame(part_data)

# Find promotional parts
promotional_parts = parts['P_CONTAINER'].str.contains('PROMO')

# Filter lineitems for promotional parts
promo_lineitems = lineitems[lineitems['L_PARTKEY'].isin(promotional_parts.index)]

# Calculate total and promotional revenue
total_revenue = lineitems['revenue'].sum()
promo_revenue = promo_lineitems['revenue'].sum()

# Calculate promotion effect percentage
promotion_effect = (promo_revenue / total_revenue) * 100

# Output result to CSV
output = pd.DataFrame([{'Promotion Effect (%)': promotion_effect}])
output.to_csv('query_output.csv', index=False)
