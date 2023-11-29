import pymongo
import pandas as pd
import direct_redis
import csv

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
db_tpch = mongo_client["tpch"]
nation_collection = db_tpch["nation"]
supplier_collection = db_tpch["supplier"]

# Query for German suppliers from MongoDB
german_nationkey = list(nation_collection.find({"N_NAME": "GERMANY"}, {"N_NATIONKEY": 1, "_id": 0}))
german_suppliers = supplier_collection.find({"S_NATIONKEY": {"$in": [n["_NATIONKEY"] for n in german_nationkey]}}, {"_id": 0})

# Convert supplier data to Pandas DataFrame
df_suppliers = pd.DataFrame(list(german_suppliers))

# Connect to Redis
dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get partsupp table from Redis
partsupp_redis_data = dr.get('partsupp')
df_partsupp = pd.read_json(partsupp_redis_data)

# Merge supplier and partsupp data frames on S_SUPPKEY
merged_df = pd.merge(df_partsupp, df_suppliers, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate the total value (PS_SUPPLYCOST * PS_AVAILQTY) and filter significant parts
merged_df['PART_VALUE'] = merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']
total_value = merged_df['PART_VALUE'].sum()
merged_df = merged_df[merged_df['PART_VALUE'] > 0.0001 * total_value]

# Sort by PART_VALUE in descending order and select relevant columns
result = merged_df.sort_values(by='PART_VALUE', ascending=False)[['PS_PARTKEY', 'PART_VALUE']]

# Write the result to CSV file
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
