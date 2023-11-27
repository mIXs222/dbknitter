import pymongo
from direct_redis import DirectRedis
import pandas as pd

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
partsupp = pd.DataFrame(list(mongo_db.partsupp.find()))

# Redis connection and data retrieval
# Assuming DirectRedis package is similar to redis and has been installed
redis_client = DirectRedis(host='redis', port=6379, db=0)
nation_raw = redis_client.get('nation')
supplier_raw = redis_client.get('supplier')

# Convert the Redis string data to Pandas DataFrame
nation = pd.read_json(nation_raw)
supplier = pd.read_json(supplier_raw)

# Filter nations for 'GERMANY' and join with suppliers
germany_nation_key = nation[nation['N_NAME'] == 'GERMANY']['N_NATIONKEY'].iloc[0]
suppliers_in_germany = supplier[supplier['S_NATIONKEY'] == germany_nation_key]

# Join partsupp with suppliers in Germany on PS_SUPPKEY == S_SUPPKEY
important_stock = partsupp.merge(suppliers_in_germany, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate total value for each part
important_stock['TOTAL_VALUE'] = important_stock['PS_AVAILQTY'] * important_stock['PS_SUPPLYCOST']

# Find the total value of all parts
total_value_of_all_parts = important_stock['TOTAL_VALUE'].sum()

# Find the parts that have significant percentage of the total value
important_stock = important_stock[important_stock['TOTAL_VALUE'] > (0.0001 * total_value_of_all_parts)]

# Selecting the required output fields
important_stock_result = important_stock[['PS_PARTKEY', 'TOTAL_VALUE']]

# Sorting the values in descending order
important_stock_result = important_stock_result.sort_values(by='TOTAL_VALUE', ascending=False)

# Writing the result to a CSV file
important_stock_result.to_csv('query_output.csv', index=False)
