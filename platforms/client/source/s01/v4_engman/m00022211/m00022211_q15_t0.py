# query.py
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Define time range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)

# MongoDB aggregation pipeline
pipeline = [
    {"$match": {
        "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
    }},
    {"$group": {
        "_id": "$L_SUPPKEY",
        "TOTAL_REVENUE": {
            "$sum": {
                "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]
            }
        }
    }},
    {"$sort": {"TOTAL_REVENUE": -1}},
]

# Perform aggregation in MongoDB to calculate revenue
lineitem_results = list(lineitem_collection.aggregate(pipeline))

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Fetch supplier data from Redis and create a DataFrame
supplier_data = r.get('supplier')
supplier_df = pd.DataFrame(supplier_data)

# Transform MongoDB results into a DataFrame
lineitem_df = pd.DataFrame(lineitem_results)

# Merge DataFrames based on supplier key
merged_df = pd.merge(supplier_df, lineitem_df, left_on='S_SUPPKEY', right_on='_id')

# Check for the top revenue
top_revenue = merged_df['TOTAL_REVENUE'].max()

# Select suppliers with revenue equal to the top revenue
top_suppliers_df = merged_df[merged_df['TOTAL_REVENUE'] == top_revenue].sort_values(
    by='S_SUPPKEY'
)

# Subset the final output columns
final_df = top_suppliers_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Write the result to CSV file
final_df.to_csv('query_output.csv', index=False)
