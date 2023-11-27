import pymongo
from direct_redis import DirectRedis
import pandas as pd

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation_coll = mongo_db["nation"]
part_coll = mongo_db["part"]
supplier_coll = mongo_db["supplier"]

# Load data from MongoDB
nation_df = pd.DataFrame(list(nation_coll.find()))
part_df = pd.DataFrame(list(part_coll.find()))
supplier_df = pd.DataFrame(list(supplier_coll.find()))
mongo_client.close()

# Filter parts matching the naming convention (assuming 'forest' in the name)
part_forest_df = part_df[part_df['P_NAME'].str.contains('forest', case=False)]

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load data from Redis
partsupp_df = pd.read_json(redis_client.get('partsupp'), orient='records')
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')
redis_client.close()

# Convert string to datetime and filter between the dates
date_start = pd.to_datetime('1994-01-01')
date_end = pd.to_datetime('1995-01-01')
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= date_start) & (lineitem_df['L_SHIPDATE'] <= date_end)
]

# Combine and filter data to match query requirements
combined_df = filtered_lineitem_df.merge(
    partsupp_df,
    how='inner',
    left_on=['L_PARTKEY', 'L_SUPPKEY'],
    right_on=['PS_PARTKEY', 'PS_SUPPKEY']
)
combined_df = combined_df.merge(
    part_forest_df,
    how='inner',
    left_on='L_PARTKEY',
    right_on='P_PARTKEY'
)
combined_df = combined_df.merge(
    supplier_df,
    how='inner',
    on='S_SUPPKEY'
)
combined_df = combined_df.merge(
    nation_df[nation_df['N_NAME'] == "CANADA"],
    how='inner',
    left_on='S_NATIONKEY',
    right_on='N_NATIONKEY'
)

# Group by S_SUPPKEY and calculate the total quantity for CANADA
grouped_suppliers = combined_df.groupby('S_SUPPKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
grouped_suppliers.rename(columns={'L_QUANTITY': 'TOTAL_QUANTITY'}, inplace=True)

# Merge to get supplier detail
supplier_totals = supplier_df.merge(
    grouped_suppliers,
    how='inner',
    left_on='S_SUPPKEY',
    right_on='S_SUPPKEY'
)

# Find suppliers with more than 50% of the forest parts shipped
supplier_totals['EXCESS_THRESHOLD'] = supplier_totals['TOTAL_QUANTITY'] > (partsupp_df.groupby('PS_SUPPKEY')['PS_AVAILQTY'].sum() * 0.5).reset_index(drop=True)
excess_suppliers = supplier_totals[supplier_totals['EXCESS_THRESHOLD']]

# Writing final result to CSV
excess_suppliers.to_csv('query_output.csv', index=False)
