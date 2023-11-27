import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Load collections from MongoDB as DataFrame
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find()))
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find()))

# Connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load Redis tables into DataFrames
nation_df = pd.read_json(redis_client.get('nation').decode('utf-8'))
part_df = pd.read_json(redis_client.get('part').decode('utf-8'))
supplier_df = pd.read_json(redis_client.get('supplier').decode('utf-8'))

# Filter parts that start with 'forest'
forest_parts = part_df[part_df['P_NAME'].str.startswith('forest')]

# Find total quantities for each part and supplier in the last year
lineitem_filtered = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1994-01-01') & (lineitem_df['L_SHIPDATE'] < '1995-01-01')]
totals = lineitem_filtered.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()
totals.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'TOTAL_QUANTITY']

# Merge partsupp, parts, and lineitem to find those with availability
partsupp_available = pd.merge(partsupp_df[partsupp_df['PS_PARTKEY'].isin(forest_parts['P_PARTKEY'])], totals, on=['PS_PARTKEY', 'PS_SUPPKEY'])
partsupp_available = partsupp_available[partsupp_available['PS_AVAILQTY'] > 0.5 * partsupp_available['TOTAL_QUANTITY']]

# Filter suppliers in Canada
suppliers_in_canada = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'])]

# Join suppliers with available parts in Canada
result = pd.merge(suppliers_in_canada[suppliers_in_canada['S_SUPPKEY'].isin(partsupp_available['PS_SUPPKEY'])], partsupp_available, how='inner', left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Select requested columns and sort
output = result[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME')

# Write result to CSV
output.to_csv('query_output.csv', index=False)
