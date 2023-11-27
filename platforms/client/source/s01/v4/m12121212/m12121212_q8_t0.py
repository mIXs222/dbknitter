from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
import csv

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Load MongoDB collections into DataFrames
nation_df = pd.DataFrame(list(mongo_db.nation.find()))
part_df = pd.DataFrame(list(mongo_db.part.find()))
orders_df = pd.DataFrame(list(mongo_db.orders.find()))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load Redis tables into DataFrames
supplier_df = pd.read_msgpack(redis_client.get('supplier'))
customer_df = pd.read_msgpack(redis_client.get('customer'))
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))
region_df = pd.read_msgpack(redis_client.get('region'))

# Filter the part and region data based on conditions
part_df = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']
region_df = region_df[region_df['R_NAME'] == 'ASIA']

# Merge all DataFrames based on query conditions
merged_df = part_df.merge(lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')
merged_df = merged_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(nation_df.rename(columns={'N_NATIONKEY': 'C_NATIONKEY'}), left_on='C_NATIONKEY', right_on='C_NATIONKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Convert ORDERDATE from string to datetime
merged_df['O_ORDERDATE'] = pd.to_datetime(merged_df['O_ORDERDATE'])
# Filter data between the given dates
merged_df = merged_df[(merged_df['O_ORDERDATE'] >= datetime(1995, 1, 1)) & (merged_df['O_ORDERDATE'] <= datetime(1996, 12, 31))]

# Create VOLUME column and O_YEAR from O_ORDERDATE
merged_df['VOLUME'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].dt.year

# Group by O_YEAR and NATION, calculate MKT_SHARE with conditions
grouped_df = merged_df.groupby('O_YEAR')
result = grouped_df.apply(lambda x: pd.Series({
    'MKT_SHARE': (x[x['N_NAME'] == 'INDIA']['VOLUME'].sum()) / x['VOLUME'].sum()
})).reset_index()

result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
