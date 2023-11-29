import pymongo
import pandas as pd
import direct_redis
import csv

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load data from MongoDB
nation_data = pd.DataFrame(list(mongo_db['nation'].find()))
supplier_data = pd.DataFrame(list(mongo_db['supplier'].find()))

# Load data from Redis
partsupp_data = pd.read_json(redis_client.get('partsupp'), orient='records')
lineitem_data = pd.read_json(redis_client.get('lineitem'), orient='records')

# Filter CANADA nations and get their nation keys
canada_nations = nation_data[nation_data['N_NAME'] == 'CANADA']['N_NATIONKEY'].tolist()

# Filter suppliers from CANADA
suppliers_in_canada = supplier_data[supplier_data['S_NATIONKEY'].isin(canada_nations)]

# Filter line items between 1994-01-01 and 1995-01-01
lineitem_data['L_SHIPDATE'] = pd.to_datetime(lineitem_data['L_SHIPDATE'])
lineitems_filtered = lineitem_data[(lineitem_data['L_SHIPDATE'] >= pd.Timestamp('1994-01-01')) & 
                                   (lineitem_data['L_SHIPDATE'] < pd.Timestamp('1995-01-01'))]

# Calculate parts shipped by each supplier
parts_shipped_by_supplier = lineitems_filtered.groupby('L_SUPPKEY')['L_QUANTITY'].sum().reset_index()

# Calculate the excess (more than 50%) of parts supplied
partsupp_data['excess'] = partsupp_data['PS_AVAILQTY'] > partsupp_data['PS_AVAILQTY'].mean() * 1.5
excess_parts = partsupp_data[partsupp_data['excess']]

# Filter out required suppliers and their excess parts
final_suppliers = suppliers_in_canada.merge(parts_shipped_by_supplier, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
final_result = final_suppliers.merge(excess_parts, left_on='S_SUPPKEY', right_on='PS_SUPPKEY', how='inner')

# Filter only forest parts (parts whose names share a certain naming convention)
# Assuming the convention is that the part name contains the word 'forest'
final_result = final_result[final_result['P_NAME'].str.contains('forest', case=False, na=False)]

# Export result to CSV
final_result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
