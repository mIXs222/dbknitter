# Import necessary libraries
import pymongo
import csv
from direct_redis import DirectRedis
import pandas as pd

# MongoDB connection and query
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
orders = mongo_db["orders"]
lineitem = mongo_db["lineitem"]

# Build pipeline to match and join the collections
pipeline = [
    {
        "$match": {
            "O_ORDERDATE": {"$lt": "1995-03-05"}
        }
    },
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "lineitems"
        }
    },
    {
        "$unwind": "$lineitems"
    },
    {
        "$match": {
            "lineitems.L_SHIPDATE": {"$gt": "1995-03-15"}
        }
    },
    {
        "$project": {
            "O_ORDERKEY": 1,
            "REVENUE": {"$multiply": ["$lineitems.L_EXTENDEDPRICE", {"$subtract": [1, "$lineitems.L_DISCOUNT"]}]},
            "O_ORDERDATE": 1,
            "O_SHIPPRIORITY": 1
        }
    },
    {
        "$sort": {"REVENUE": -1}
    }
]

# Execute the pipeline
mongo_results = list(orders.aggregate(pipeline))

# Redis connection
r = DirectRedis(host="redis", port=6379, db=0)
df_customer = pd.read_json(r.get('customer'), orient='records')

# Filter customers with market segment BUILDING
df_customer = df_customer[df_customer['C_MKTSEGMENT'] == 'BUILDING']

# Convert mongo results to DataFrame
df_orders = pd.DataFrame(mongo_results)

# Merge mongo and redis dataframes based on customer key
merged_df = df_orders.merge(df_customer, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Select the necessary columns
final_df = merged_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

# Write the output to a CSV file
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)

# Close the mongo client
mongo_client.close()
