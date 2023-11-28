import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client["tpch"]

# Query the 'lineitem' collection for shipping dates between Jan 1, 1996, and Mar 31, 1996
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {
                "$gte": "1996-01-01",
                "$lte": "1996-03-31"
            }
        }
    },
    {
        "$group": {
            "_id": "$L_SUPPKEY",
            "total_revenue": {
                "$sum": {
                    "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]
                }
            }
        }
    }
]

lineitems_revenue = list(db.lineitem.aggregate(pipeline))
df_lineitems_revenue = pd.DataFrame(lineitems_revenue).rename(columns={"_id": "SUPPLIER_NO", "total_revenue": "TOTAL_REVENUE"})

# Connect to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Get 'supplier' data from Redis
suppliers_data = eval(redis_client.get('supplier'))
df_suppliers = pd.DataFrame(suppliers_data)

# Prepare Redis dataframe
df_suppliers = df_suppliers.rename(columns=lambda x: x[2:])
df_suppliers['SUPPLIER_NO'] = df_suppliers['SUPPKEY']

# Merge dataframes to include supplier information
df_merged = pd.merge(df_suppliers, df_lineitems_revenue, on="SUPPLIER_NO")

# Find the supplier with the maximum total revenue
max_revenue_supplier = df_merged[df_merged['TOTAL_REVENUE'] == df_merged['TOTAL_REVENUE'].max()]

# Order by 'S_SUPPKEY'
max_revenue_supplier.sort_values(by='S_SUPPKEY', ascending=True, inplace=True)

# Save to csv
max_revenue_supplier.to_csv('query_output.csv', index=False)
