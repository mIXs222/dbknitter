# query_code.py

import pymongo
import redis
import pandas as pd
from datetime import datetime
from io import StringIO
import direct_redis

# Establish a connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['tpch']

# Retrieve lineitem data from MongoDB with the shipping date criteria
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 9, 30)
query = {
    "L_SHIPDATE": {
        "$gte": start_date,
        "$lte": end_date
    }
}
lineitem_fields = {
    "_id": False
}
mongo_lineitems = pd.DataFrame(list(mongo_db.lineitem.find(query, lineitem_fields)))

# Establish a connection to Redis using direct_redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part_keys = redis_client.keys('part:*')

# Retrieve part data from Redis
redis_parts = []

for key in part_keys:
    data = redis_client.get(key)
    if data:
        redis_parts.append(pd.read_json(StringIO(data), typ='series'))

df_parts = pd.DataFrame(redis_parts)

# Filter parts with types that start with 'PROMO'
promo_parts = df_parts[df_parts['P_TYPE'].str.startswith('PROMO')]

# Merge with lineitem data based on part keys
merged_data = pd.merge(mongo_lineitems, promo_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate promotional revenue
merged_data['ADJUSTED_PRICE'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])
promo_revenue = merged_data['ADJUSTED_PRICE'].sum()

# Calculate the total revenue for all line items during the timeframe
mongo_lineitems['ADJUSTED_PRICE'] = mongo_lineitems['L_EXTENDEDPRICE'] * (1 - mongo_lineitems['L_DISCOUNT'])
total_revenue = mongo_lineitems['ADJUSTED_PRICE'].sum()

# Calculate promotional revenue as a percentage of total revenue
promo_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Output the results into query_output.csv
with open('query_output.csv', 'w') as file:
    file.write(f"Promotional Revenue Percentage,{promo_percentage}\n")

