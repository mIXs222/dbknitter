import pymongo
import pandas as pd
import redis
import direct_redis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_tpch_db = mongo_client["tpch"]
nation_collection = mongo_tpch_db["nation"]
supplier_collection = mongo_tpch_db["supplier"]

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get data from MongoDB
nation_df = pd.DataFrame(list(nation_collection.find({}, {"_id": 0})))
supplier_df = pd.DataFrame(list(supplier_collection.find({}, {"_id": 0})))

# Get data from Redis
partsupp_df = pd.read_json(redis_client.get('partsupp').decode('utf-8'))

# Filter suppliers located in Germany
german_suppliers = nation_df[nation_df['N_NAME'] == 'GERMANY']['N_NATIONKEY']
suppliers_in_germany_df = supplier_df[supplier_df['S_NATIONKEY'].isin(german_suppliers)]

# Merge partsupp and supplier data
parts_suppliers_germany_df = partsupp_df.merge(suppliers_in_germany_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate total value for each part from suppliers in Germany
parts_suppliers_germany_df['TOTAL_VALUE'] = parts_suppliers_germany_df['PS_SUPPLYCOST'] * parts_suppliers_germany_df['PS_AVAILQTY']
total_value_threshold = parts_suppliers_germany_df['TOTAL_VALUE'].sum() * 0.05  # Assuming the threshold is 5% of the overall value

# Group by part key and filter by having the sum of values surpass the threshold
grouped_parts = parts_suppliers_germany_df.groupby('PS_PARTKEY').filter(lambda x: x['TOTAL_VALUE'].sum() > total_value_threshold)
final_results = grouped_parts.groupby('PS_PARTKEY', as_index=False).sum()

# Select necessary columns and sort by total value in descending order
final_results = final_results[['PS_PARTKEY', 'TOTAL_VALUE']].sort_values(by='TOTAL_VALUE', ascending=False)

# Export to CSV
final_results.to_csv('query_output.csv', index=False)
