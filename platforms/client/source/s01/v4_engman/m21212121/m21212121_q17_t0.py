# query.py
from pymongo import MongoClient
import direct_redis
import pandas as pd

# MongoDB connection and data retrieval
client = MongoClient("mongodb", 27017)
mongodb_db = client["tpch"]
lineitem_collection = mongodb_db["lineitem"]

# Aggregate to compute average quantity for the BRAND#23 and MED BAG
pipeline = [
    {
        "$match": {
            "L_LINESTATUS": {"$in": ["F", "O"]},  # considering all orders (past and pending)
        }
    },
    {
        "$group": {
            "_id": None,
            "average_quantity": {"$avg": "$L_QUANTITY"}
        }
    }
]
average_quantity_result = list(lineitem_collection.aggregate(pipeline))
average_quantity = average_quantity_result[0]["average_quantity"] if average_quantity_result else 0

# Redis connection and data retrieval
redis_connection = direct_redis.DirectRedis(host="redis", port=6379, db=0)
part_records = redis_connection.get('part')
part_df = pd.read_json(part_records)

# Filter parts with BRAND#23 and MED BAG
filtered_parts = part_df[(part_df["P_BRAND"] == "BRAND#23") & (part_df["P_CONTAINER"] == "MED BAG")]

# Join MongoDB and Redis dataframes
lineitems_df = pd.DataFrame(list(lineitem_collection.find({}, {'_id': 0})))

# Filter lineitems with parts of interest
lineitems_filtered = lineitems_df[lineitems_df['L_PARTKEY'].isin(filtered_parts['P_PARTKEY'])]

# Calculate loss in revenue
threshold_quantity = average_quantity * 0.20
loss_lineitems = lineitems_filtered[lineitems_filtered['L_QUANTITY'] < threshold_quantity]
loss_lineitems['gross_loss'] = loss_lineitems['L_EXTENDEDPRICE'] - (loss_lineitems['L_EXTENDEDPRICE'] * loss_lineitems['L_DISCOUNT'])

# Calculate average yearly loss by considering the lifespan of the dataset (7 years)
average_yearly_loss = loss_lineitems['gross_loss'].sum() / 7

# Write the average yearly loss to the file query_output.csv
output_df = pd.DataFrame([{'average_yearly_loss': average_yearly_loss}])
output_df.to_csv('query_output.csv', index=False)
