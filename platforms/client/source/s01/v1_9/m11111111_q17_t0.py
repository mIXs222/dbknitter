from pymongo import MongoClient
import pandas as pd

# Establish a connection to the MongoDB
client = MongoClient("mongodb://mongodb:27017/")
db = client['tpch']

# create empty DataFrame
output_df = pd.DataFrame()

# Get lineitem and part collection
lineItems = db.lineitem
parts = db.part

pipeline = [
    {"$lookup": {
        "from": "part",
        "localField": "L_PARTKEY",
        "foreignField": "P_PARTKEY",
        "as": "part"
    }},
    {"$unwind": "$part"},
    {"$match": {
        "part.P_BRAND": "Brand#23",
        "part.P_CONTAINER": "MED BAG"
    }},
    {"$group": {
        "_id": "$part.P_PARTKEY",
        "avg_qty": {"$avg": "$L_QUANTITY"},
        "total_price": {"$sum": "$L_EXTENDEDPRICE"}
    }}
]

# Aggregate data (perform join operation and compute total extended price and average quantity.)
aggregated_data = lineItems.aggregate(pipeline)

# Iterate over data
for data in aggregated_data:
    if data['avg_qty']*0.2 > data['total_price']:
        single_row = pd.DataFrame({'AVG_YEARLY': data['total_price']}, index=[0])
        output_df = pd.concat([output_df, single_row])

# Ensure the output is divided by 7.0 to get AVG_YEARLY
output_df['AVG_YEARLY'] = output_df['AVG_YEARLY'] / 7.0

# Write to csv
output_df.to_csv('query_output.csv', index=False)
