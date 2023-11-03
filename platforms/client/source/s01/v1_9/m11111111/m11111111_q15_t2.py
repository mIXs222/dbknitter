from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

# Connect to MongoDB
client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Define start and end dates
start_date = datetime.strptime('1996-01-01', '%Y-%m-%d')
end_date = start_date + relativedelta(months=+3)

# Prepare aggregation pipeline
pipeline = [
    {"$match": {
        "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
    }},
    {"$group": {
        "_id": "$L_SUPPKEY",
        "TOTAL_REVENUE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}},
    }},
]

# Execute aggregation
revenue0 = list(db['lineitem'].aggregate(pipeline))

supplier_ids = [rev['_id'] for rev in revenue0]
max_revenue = max(rev['TOTAL_REVENUE'] for rev in revenue0)

# Fetch suppliers
result_suppliers = db['supplier'].find({
    'S_SUPPKEY': {'$in': supplier_ids}
})

# Prepare result
results = []
for supplier in result_suppliers:
    for revenue in revenue0:
        if supplier['S_SUPPKEY'] == revenue['_id'] and revenue['TOTAL_REVENUE'] == max_revenue:
            results.append({
                'S_SUPPKEY': supplier['S_SUPPKEY'],
                'S_NAME': supplier['S_NAME'],
                'S_ADDRESS': supplier['S_ADDRESS'],
                'S_PHONE': supplier['S_PHONE'],
                'TOTAL_REVENUE': revenue['TOTAL_REVENUE'],
            })

# Write to CSV
df = pd.DataFrame(results)
df.to_csv('query_output.csv', index=False)
