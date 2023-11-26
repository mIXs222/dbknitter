from pymongo import MongoClient
import direct_redis
import pandas as pd

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
db = mongo_client.tpch
# Fetch nation and supplier data
nation_data = list(db.nation.find({'N_NAME': 'GERMANY'}))
supplier_data = list(db.supplier.find())

# Convert to DataFrame
df_nation = pd.DataFrame(nation_data)
df_supplier = pd.DataFrame(supplier_data)

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379)
# Fetch partsupp data
partsupp_data = r.get('partsupp')
df_partsupp = pd.read_json(partsupp_data)

# Data transformation: Merge data from MongoDB and partsupp from Redis
df_merged = pd.merge(df_partsupp, df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
df_final = pd.merge(df_merged, df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Perform the calculation
df_grouped = df_final.groupby('PS_PARTKEY').agg(VALUE=('PS_SUPPLYCOST', lambda x: (x*df_final['PS_AVAILQTY']).sum()))
total_value = df_final['PS_SUPPLYCOST'].sum() * df_final['PS_AVAILQTY'].sum() * 0.0001000000
df_grouped = df_grouped[df_grouped['VALUE'] > total_value]
df_grouped = df_grouped.sort_values('VALUE', ascending=False)

# Output result to CSV
df_grouped.to_csv('query_output.csv')
