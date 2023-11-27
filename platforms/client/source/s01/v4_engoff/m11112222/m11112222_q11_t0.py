import pymongo
import csv
import pandas as pd
import redis.clients.jedis.exceptions
import direct_redis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
collection_nation = mongo_db["nation"]
collection_supplier = mongo_db["supplier"]

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the N_NATIONKEY for 'GERMANY'
nation_key = collection_nation.find_one({"N_NAME": "GERMANY"})["N_NATIONKEY"]

# Get all suppliers from 'GERMANY'
suppliers = list(collection_supplier.find({"S_NATIONKEY": nation_key}))

# Get partsupp data from Redis (assuming each row is a JSON encoded string)
partsupp_rows = []
for supplier in suppliers:
    suppkey = supplier["S_SUPPKEY"]
    part_row_str = redis_client.get(f'partsupp:{suppkey}')
    if part_row_str is not None:
        partsupp_rows.append(json.loads(part_row_str))

# Convert to DataFrame
df_partsupp = pd.DataFrame(partsupp_rows)

# Calculate total value and find parts with significant percentage
df_partsupp['TOTAL_VALUE'] = df_partsupp['PS_AVAILQTY'] * df_partsupp['PS_SUPPLYCOST']
total_value = df_partsupp['TOTAL_VALUE'].sum()
df_significant = df_partsupp[df_partsupp['TOTAL_VALUE'] > total_value * 0.0001]

# Filter and sort the relevant data
df_result = df_significant[['PS_PARTKEY', 'TOTAL_VALUE']].sort_values(by='TOTAL_VALUE', ascending=False)

# Write the output to a CSV file
df_result.to_csv('query_output.csv', index=False)

# Close the MongoDB connection
mongo_client.close()
