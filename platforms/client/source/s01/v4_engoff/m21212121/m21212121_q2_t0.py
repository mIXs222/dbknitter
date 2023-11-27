import pymongo
import pandas as pd
import direct_redis
import csv

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client['tpch']
collection_region = mongo_db['region']
collection_supplier = mongo_db['supplier']

# Fetch data from MongoDB
regions = pd.DataFrame(list(collection_region.find()))
suppliers = pd.DataFrame(list(collection_supplier.find()))

# Filter suppliers in 'EUROPE' region
europe_region_keys = regions[regions['R_NAME'] == 'EUROPE']['R_REGIONKEY']
europe_suppliers = suppliers[suppliers['S_NATIONKEY'].isin(europe_region_keys)]

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis
nation = pd.read_json(r.get('nation'))
part = pd.read_json(r.get('part'))
partsupp = pd.read_json(r.get('partsupp'))

# Filter part with 'BRASS' type and size of 15
brass_parts = part[(part['P_TYPE'] == 'BRASS') & (part['P_SIZE'] == 15)]

# Join and filter to get the minimum cost supplier information
result = pd.merge(brass_parts, partsupp, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
result = result.groupby(['PS_PARTKEY']).apply(lambda x: x[x.PS_SUPPLYCOST == x.PS_SUPPLYCOST.min()])

# Join with suppliers and nation
final_result = result.merge(europe_suppliers, how='left', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
final_result = final_result.merge(nation, how='left', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Select and rename columns
final_result = final_result[['S_ACCTBAL', 'S_NAME_x', 'N_NAME', 'PS_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT_x']]
final_result.columns = ['Supplier Account Balance', 'Supplier Name', 'Nation Name', 'Part Number', 'Manufacturer', 'Supplier Address', 'Phone Number', 'Comment']

# Sort as specified in query
final_result.sort_values(by=['Supplier Account Balance', 'Nation Name', 'Supplier Name', 'Part Number'], ascending=[False, True, True, True], inplace=True)

# Write output to CSV
final_result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
