import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Constants
MONGODB_HOST = 'mongodb'
MONGODB_PORT = 27017
REDIS_HOST = 'redis'
REDIS_PORT = 6379
THRESHOLD_PERCENTAGE = 0.05  # Example threshold, as the actual value wasn't specified

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
mongo_db = mongo_client['tpch']

# Fetch 'nation' data from MongoDB
nation_df = pd.DataFrame(list(mongo_db['nation'].find()))

# Connect to Redis
redis_client = DirectRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Fetch 'supplier' data from Redis
supplier_df = pd.read_msgpack(redis_client.get('supplier'))

# Fetch 'partsupp' data from Redis
partsupp_df = pd.read_msgpack(redis_client.get('partsupp'))

# Merge and filter dataframes
merged_df = supplier_df.merge(partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
germany_nationkey = nation_df[nation_df['N_NAME'] == 'GERMANY'].iloc[0]['N_NATIONKEY']
germany_suppliers = merged_df[merged_df['S_NATIONKEY'] == germany_nationkey]

# Calculate total value and threshold
germany_suppliers['TOTAL_VALUE'] = germany_suppliers['PS_SUPPLYCOST'] * germany_suppliers['PS_AVAILQTY']
total_value_in_germany = germany_suppliers['TOTAL_VALUE'].sum()
threshold_value = total_value_in_germany * THRESHOLD_PERCENTAGE

# Group by part key and filter using the 'HAVING' condition
grouped = germany_suppliers.groupby('PS_PARTKEY').agg({'TOTAL_VALUE': 'sum'})
filtered_grouped = grouped[grouped['TOTAL_VALUE'] > threshold_value]

# Order the results and write to CSV
filtered_grouped = filtered_grouped.sort_values('TOTAL_VALUE', ascending=False)
filtered_grouped.to_csv('query_output.csv')
