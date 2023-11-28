from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Load MongoDB tables
region_df = pd.DataFrame(list(mongo_db['region'].find()))
supplier_df = pd.DataFrame(list(mongo_db['supplier'].find()))
customer_df = pd.DataFrame(list(mongo_db['customer'].find()))
lineitem_df = pd.DataFrame(list(mongo_db['lineitem'].find()))

# Load Redis tables
nation_df = pd.read_json(redis.get('nation').decode())
part_df = pd.read_json(redis.get('part').decode())
orders_df = pd.read_json(redis.get('orders').decode())

# Filter the data
part_df = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']
asian_nations = nation_df[nation_df['N_REGIONKEY'] == region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY'].iloc[0]]
indian_nation = asian_nations[asian_nations['N_NAME'] == 'INDIA']

# Merge the tables
filtered_lineitems = lineitem_df[lineitem_df['L_PARTKEY'].isin(part_df['P_PARTKEY'])]
filtered_supplier = supplier_df[supplier_df['S_NATIONKEY'].isin(asian_nations['N_NATIONKEY'])]

filtered_df = (
    filtered_lineitems
    .merge(filtered_supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
)

# Compute the volume and filter by years 1995 and 1996
filtered_df['L_VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
filtered_df['YEAR'] = filtered_df['L_SHIPDATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').year)
filtered_df = filtered_df[(filtered_df['YEAR'] == 1995) | (filtered_df['YEAR'] == 1996)]

# Calculate market share
total_volume_by_year = filtered_df.groupby('YEAR')['L_VOLUME'].sum().reset_index()
india_volume_by_year = filtered_df[filtered_df['C_NATIONKEY'].isin(indian_nation['N_NATIONKEY'])].groupby('YEAR')['L_VOLUME'].sum().reset_index()
market_share_by_year = india_volume_by_year.merge(total_volume_by_year, on='YEAR')
market_share_by_year['MARKET_SHARE'] = market_share_by_year['L_VOLUME_x'] / market_share_by_year['L_VOLUME_y']

# Select only relevant columns and sort
market_share_by_year = market_share_by_year[['YEAR', 'MARKET_SHARE']].sort_values('YEAR')

# Write to CSV
market_share_by_year.to_csv('query_output.csv', index=False)
