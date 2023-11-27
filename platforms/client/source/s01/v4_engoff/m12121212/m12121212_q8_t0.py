# Filename: market_share_query.py
import pymongo
import pandas as pd
from bson.son import SON  # Needed for aggregation order guarantees in pymongo
import direct_redis

# MongoDB connection and data fetching
client = pymongo.MongoClient("mongodb", 27017)
db = client['tpch']

# Retrieve necessary data from MongoDB
nation_data = pd.DataFrame(list(db.nation.find()))
part_data = pd.DataFrame(list(db.part.find()))
orders_data = pd.DataFrame(list(db.orders.find()))

# Filter parts to get SMALL PLATED COPPER
part_data_sp_copper = part_data[part_data['P_TYPE'] == 'SMALL PLATED COPPER']

# Redis connection and data fetching
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve necessary data from Redis
region_data = pd.DataFrame(r.get('region'))
supplier_data = pd.DataFrame(r.get('supplier'))
lineitem_data = pd.DataFrame(r.get('lineitem'))

# Ensure the columns are of the correct type
lineitem_data[['L_EXTENDEDPRICE', 'L_DISCOUNT']] = lineitem_data[['L_EXTENDEDPRICE', 'L_DISCOUNT']].apply(pd.to_numeric)

# Merge dataframes to process the query
combined_df = (
    lineitem_data.merge(supplier_data, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation_data, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(region_data, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
    .merge(part_data_sp_copper, left_on='L_PARTKEY', right_on='P_PARTKEY')
)

# Process the market share for the years 1995 and 1996
output = combined_df[
    combined_df['R_NAME'] == 'ASIA'
].assign(
    YEAR=lambda x: pd.to_datetime(x['O_ORDERDATE']).dt.year,
    REVENUE=lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])
).query(
    "YEAR == 1995 or YEAR == 1996"
).query(
    "N_NAME == 'INDIA'"
).groupby(
    'YEAR'
)['REVENUE'].sum().reset_index()

output.columns = ['YEAR', 'MARKET_SHARE']

# Write the query output to a CSV file
output.to_csv('query_output.csv', index=False)

client.close()
