from pymongo import MongoClient
import pandas as pd

# Instantiate MongoClient
client = MongoClient("mongodb://mongodb:27017")

# Get database
db = client['tpch']

# Here we use MongoDB's aggregation pipeline to construct a similar query as your SQL.
pipeline = [
    {"$lookup": {
        "from": "part",
        "localField": "L_PARTKEY",
        "foreignField": "P_PARTKEY",
        "as": "part"
    }},
    {"$lookup": {
        "from": "supplier",
        "localField": "L_SUPPKEY",
        "foreignField": "S_SUPPKEY",
        "as": "supplier"
    }},
    {"$lookup": {
        "from": "orders",
        "localField": "L_ORDERKEY",
        "foreignField": "O_ORDERKEY",
        "as": "orders"
    }},
    {"$lookup": {
        "from": "customer",
        "localField": "orders.O_CUSTKEY",
        "foreignField": "C_CUSTKEY",
        "as": "customer"
    }},
    {"$lookup": {
        "from": "nation",
        "localField": "customer.C_NATIONKEY",
        "foreignField": "N_NATIONKEY",
        "as": "nation1"
    }},
    {"$lookup": {
        "from": "nation",
        "localField": "supplier.S_NATIONKEY",
        "foreignField": "N_NATIONKEY",
        "as": "nation2"
    }},
    {"$lookup": {
        "from": "region",
        "localField": "nation1.N_REGIONKEY",
        "foreignField": "R_REGIONKEY",
        "as": "region"
    }},
    {"$match": {
        "part.P_TYPE": "SMALL PLATED COPPER",
        "region.R_NAME": "ASIA",
        "orders.O_ORDERDATE": {"$gte": "1995-01-01", "$lte": "1996-12-31"}
    }},
    {"$group": {
        "_id": {
            "O_YEAR": {"$year": "$orders.O_ORDERDATE"},
            "NATION": "$nation2.N_NAME"
        },
        "VOLUME": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}}
    }}
]

# Perform aggregation
result = list(db.lineitem.aggregate(pipeline))

# This dataframe will have columns: O_YEAR, NATION, VOLUME
df = pd.json_normalize(result)

# Now we can further process this dataframe to calculate MKT_SHARE
df_output = df.groupby('_id.O_YEAR').apply(lambda df: df['VOLUME'].where(df['_id.NATION']=='INDIA', 0).sum()/df['VOLUME'].sum()).reset_index()
df_output.columns = ['O_YEAR', 'MKT_SHARE']

# Write csv
df_output.to_csv('query_output.csv', index=False)
