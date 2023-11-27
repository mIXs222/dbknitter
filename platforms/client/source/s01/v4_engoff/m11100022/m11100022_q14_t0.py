import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
import csv

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_collection = mongo_db["part"]

# Query MongoDB for parts that are considered promotional
query = {
    "P_NAME": {"$regex": ".*promo.*", "$options": "i"}  # Assuming 'promo' denotes promotional parts
}
promotional_parts = list(mongo_collection.find(query, {"_id": 0, "P_PARTKEY": 1}))
promotional_partkeys = [p['P_PARTKEY'] for p in promotional_parts]

# Establish connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Read lineitem table from Redis into Pandas DataFrame
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter orders shipped in the given date range
start_date = datetime.strptime("1995-09-01", "%Y-%m-%d")
end_date = datetime.strptime("1995-10-01", "%Y-%m-%d")
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_filtered = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) & 
    (lineitem_df['L_SHIPDATE'] <= end_date)
]

# Calculate revenue from promotional parts
lineitem_filtered['L_REVENUE'] = lineitem_filtered['L_EXTENDEDPRICE'] * (1 - lineitem_filtered['L_DISCOUNT'])
promo_revenue = lineitem_filtered[
    lineitem_filtered['L_PARTKEY'].isin(promotional_partkeys)
]['L_REVENUE'].sum()

# Calculate total revenue in the same date range
total_revenue = lineitem_filtered['L_REVENUE'].sum()

# Calculate the promotion effect percentage
promotion_effect = (promo_revenue / total_revenue) * 100 if total_revenue != 0 else 0

# Output result
output = {'Promotion Effect (%)': promotion_effect}

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=output.keys())
    writer.writeheader()
    writer.writerow(output)

# Clean up connections
mongo_client.close()
redis_client.close()
