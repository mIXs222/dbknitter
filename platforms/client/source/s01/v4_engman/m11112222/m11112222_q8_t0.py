import pymongo
import direct_redis
import pandas as pd

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Get data from MongoDB collections
nation_df = pd.DataFrame(list(mongo_db.nation.find()))
region_df = pd.DataFrame(list(mongo_db.region.find()))
part_df = pd.DataFrame(list(mongo_db.part.find()))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()))

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
customer_df = pd.read_json(redis_client.get('customer'))
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Merge and process data
asia_df = region_df[region_df['R_NAME'] == 'ASIA']
india_nation_df = nation_df[nation_df['N_NAME'] == 'INDIA']

# Merge to get suppliers from INDIA in region ASIA
india_suppliers_df = supplier_df.merge(india_nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Merge to get parts of type 'SMALL PLATED COPPER'
small_plated_copper_parts_df = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']

# Merge to get relevant lineitems
relevant_lineitems_df = lineitem_df.merge(
    small_plated_copper_parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Merge with orders to get dates, then filter for years 1995 and 1996
relevant_orders_df = orders_df[
    (pd.to_datetime(orders_df['O_ORDERDATE']).dt.year == 1995) |
    (pd.to_datetime(orders_df['O_ORDERDATE']).dt.year == 1996)
]
relevant_lineitems_df = relevant_lineitems_df.merge(relevant_orders_df[['O_ORDERKEY', 'O_ORDERDATE']], on='O_ORDERKEY')

# Calculate revenue from INDIA
relevant_lineitems_df['revenue'] = relevant_lineitems_df['L_EXTENDEDPRICE'] * (1 - relevant_lineitems_df['L_DISCOUNT'])
india_revenue_df = relevant_lineitems_df.merge(india_suppliers_df[['S_SUPPKEY']], left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Group by year and calculate market share
market_share_df = india_revenue_df.groupby(pd.to_datetime(india_revenue_df['O_ORDERDATE']).dt.year)['revenue'].sum().reset_index()
market_share_df.columns = ['order year', 'market share']
total_revenue_df = relevant_lineitems_df.groupby(pd.to_datetime(relevant_lineitems_df['O_ORDERDATE']).dt.year)['revenue'].sum().reset_index()
market_share_df = market_share_df.merge(total_revenue_df, on='order year')
market_share_df['market share'] = market_share_df['market share_x'] / market_share_df['market share_y']
market_share_df = market_share_df[['order year', 'market share']]

# Write to CSV
market_share_df.to_csv('query_output.csv', index=False)
