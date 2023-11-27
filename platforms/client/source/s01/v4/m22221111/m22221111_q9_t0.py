# query.py
from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
import pandas as pd
from datetime import datetime
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Retrieve MongoDB tables
partsupp = pd.DataFrame(list(mongodb.partsupp.find()))
orders = pd.DataFrame(list(mongodb.orders.find()))
lineitem = pd.DataFrame(list(mongodb.lineitem.find()))

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Retrieve Redis tables and convert them into Pandas DataFrame
nation = pd.read_msgpack(redis.get('nation'))
part = pd.read_msgpack(redis.get('part'))
supplier = pd.read_msgpack(redis.get('supplier'))

# Perform the query logic
# First, we filter parts with names like '%dim%'
part_dim = part[part['P_NAME'].str.contains('dim')]

# Perform necessary joins
merged_df = (lineitem
             .merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
             .merge(supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
             .merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
             .merge(part_dim, left_on='L_PARTKEY', right_on='P_PARTKEY')
             .merge(partsupp, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY']))

# Calculate 'AMOUNT'
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']

# Extract year from O_ORDERDATE
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').strftime('%Y'))

# Group by NATION and O_YEAR, and calculate SUM_PROFIT
result = merged_df.groupby(['NATION', 'O_YEAR']).agg(SUM_PROFIT=('AMOUNT', 'sum')).reset_index()

# Order by NATION and O_YEAR DESC
result = result.sort_values(['NATION', 'O_YEAR'], ascending=[True, False])

# Write output to CSV
result.to_csv('query_output.csv', index=False)
