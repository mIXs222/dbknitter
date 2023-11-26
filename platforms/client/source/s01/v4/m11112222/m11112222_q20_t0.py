from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# MongoDB connection and data retrieval
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
# Extract the necessary collections
parts = pd.DataFrame(list(mongo_db.part.find({'P_NAME': {'$regex': '^forest'}}, {'_id': 0, 'P_PARTKEY': 1})))
suppliers = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_NATIONKEY': 1})))
nations = pd.DataFrame(list(mongo_db.nation.find({'N_NAME': 'CANADA'}, {'_id': 0, 'N_NATIONKEY': 1})))
mongo_client.close()

# Use direct_redis.DirectRedis instead of redis.Redis
from direct_redis import DirectRedis
redis_client = DirectRedis(host='redis', port=6379, db=0)
# Convert the strings retrieved from Redis to DataFrames
partsupp = pd.read_json(redis_client.get('partsupp'))
lineitem = pd.read_json(redis_client.get('lineitem'))
redis_client.close()

# Specific year filter for the lineitem shipping dates
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
lineitem_filtered = lineitem[(lineitem['L_SHIPDATE'] >= start_date) & (lineitem['L_SHIPDATE'] < end_date)]

# SQL-like query execution using pandas merge and query functions
# Equivalent of the nested SELECTs in the original SQL query.
avail_partsupp = partsupp[partsupp.PS_PARTKEY.isin(parts.P_PARTKEY)]
grouped_lineitem = lineitem_filtered.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index(name='SUM_L_QUANTITY')
grouped_lineitem['HALF_SUM_QUANTITY'] = grouped_lineitem['SUM_L_QUANTITY'] * 0.5

# Merge and filter on the condition about quantity
avail_partsupp = avail_partsupp.merge(grouped_lineitem, how='left', left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
avail_partsupp = avail_partsupp[avail_partsupp.PS_AVAILQTY > avail_partsupp.HALF_SUM_QUANTITY]

# Final merge with suppliers and nations
result = suppliers.merge(nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
result = result[result.S_SUPPKEY.isin(avail_partsupp.PS_SUPPKEY)]

# Sorting the result
result = result[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME')

# Write the output to a CSV file
result.to_csv('query_output.csv', index=False)
