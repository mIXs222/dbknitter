import pymongo
import pandas as pd
import direct_redis

# Constants for MongoDB
MONGO_DB_NAME = 'tpch'
MONGO_PORT = 27017
MONGO_HOSTNAME = 'mongodb'

# Connect to MongoDB
mongo_client = pymongo.MongoClient(MONGO_HOSTNAME, MONGO_PORT)
mongo_db = mongo_client[MONGO_DB_NAME]
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find()))

# Constants for Redis
REDIS_DB_NAME = 0
REDIS_PORT = 6379
REDIS_HOSTNAME = 'redis'

# Connect to Redis
redis_client = direct_redis.DirectRedis(host=REDIS_HOSTNAME, port=REDIS_PORT, db=REDIS_DB_NAME)
nation_df = pd.DataFrame(eval(redis_client.get('nation')))
supplier_df = pd.DataFrame(eval(redis_client.get('supplier')))

# Filter the suppliers based on the nation (Germany)
german_nation_keys = nation_df[nation_df['N_NAME'] == 'GERMANY']['N_NATIONKEY'].tolist()
german_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(german_nation_keys)]

# Join the partsupp and supplier dataframes
parts_in_germany = pd.merge(partsupp_df, german_suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate the value of the parts
parts_in_germany['VALUE'] = parts_in_germany['PS_AVAILQTY'] * parts_in_germany['PS_SUPPLYCOST']

# Filter parts that represent a significant percentage of the total value
total_value = parts_in_germany['VALUE'].sum()
threshold = 0.0001
important_parts = parts_in_germany[parts_in_germany['VALUE'] > total_value * threshold]

# Select the relevant columns
important_parts = important_parts[['PS_PARTKEY', 'VALUE']]

# Sort the parts by value
important_parts_sorted = important_parts.sort_values(by='VALUE', ascending=False)

# Output the results to a CSV file
important_parts_sorted.to_csv('query_output.csv', index=False)
