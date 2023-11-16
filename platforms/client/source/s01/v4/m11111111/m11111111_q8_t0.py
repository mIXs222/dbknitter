from pymongo import MongoClient
import pandas as pd
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Get the necessary collections
nation = db['nation']
region = db['region']
part = db['part']
supplier = db['supplier']
customer = db['customer']
orders = db['orders']
lineitem = db['lineitem']

# Query to fetch the data, you may need to adjust the pipeline according to your schema
pipeline = [
    # Join operations
    {"$lookup": {
        "from": "region",
        "localField": "N_REGIONKEY",
        "foreignField": "R_REGIONKEY",
        "as": "region_join"
    }},
    {"$unwind": "$region_join"},
    {"$lookup": {
        "from": "customer",
        "localField": "N_NATIONKEY",
        "foreignField": "C_NATIONKEY",
        "as": "customer_join"
    }},
    {"$unwind": "$customer_join"},
    {"$lookup": {
        "from": "orders",
        "localField": "customer_join.C_CUSTKEY",
        "foreignField": "O_CUSTKEY",
        "as": "orders_join"
    }},
    {"$unwind": "$orders_join"},
    {"$lookup": {
        "from": "supplier",
        "localField": "N_NATIONKEY",
        "foreignField": "S_NATIONKEY",
        "as": "supplier_join"
    }},
    {"$unwind": "$supplier_join"},
    {"$lookup": {
        "from": "lineitem",
        "localField": "orders_join.O_ORDERKEY",
        "foreignField": "L_ORDERKEY",
        "as": "lineitem_join"
    }},
    {"$unwind": "$lineitem_join"},
    {"$lookup": {
        "from": "part",
        "localField": "lineitem_join.L_PARTKEY",
        "foreignField": "P_PARTKEY",
        "as": "part_join"
    }},
    {"$unwind": "$part_join"},
    # Conditions
    {"$match": {
        "region_join.R_NAME": "ASIA",
        "part_join.P_TYPE": "SMALL PLATED COPPER",
        "orders_join.O_ORDERDATE": {"$gte": "1995-01-01", "$lte": "1996-12-31"}
    }},
    # Project the required fields
    {"$project": {
        "O_YEAR": {"$year": "$orders_join.O_ORDERDATE"},
        "VOLUME": {"$multiply": ["$lineitem_join.L_EXTENDEDPRICE", {"$subtract": [1, "$lineitem_join.L_DISCOUNT"]}]},
        "NATION": "$N_NAME"
    }},
]

# Run aggregation
cursor = list(nation.aggregate(pipeline))

# Convert to DataFrame
df = pd.DataFrame(cursor)

# Calculate Market Share
df['VOLUME'] = df.apply(lambda x: x['VOLUME'] if x['NATION'] == 'INDIA' else 0, axis=1)
result = df.groupby('O_YEAR', as_index=False).agg({'VOLUME': 'sum'})
result.rename(columns={'VOLUME': 'MKT_SHARE'}, inplace=True)

# Save the result to a CSV file
result.to_csv('query_output.csv', index=False)
