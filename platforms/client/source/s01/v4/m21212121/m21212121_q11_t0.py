import pymongo
import redis_functions
import pandas as pd

# Establish a connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
supplier_col = mongo_db["supplier"]

# Fetch supplier data from MongoDB and create DataFrame
supplier_data = list(supplier_col.find({}, {'_id': 0}))
df_supplier = pd.DataFrame(supplier_data)

# Establish a connection to Redis
redis_client = redis_functions.DirectRedis(host='redis', port=6379, db=0)

# Fetch nation and partsupp data from Redis
df_nation = pd.read_json(redis_client.get('nation'))
df_partsupp = pd.read_json(redis_client.get('partsupp'))

# Inner join of supplier and nation on NATIONKEY and SUPPLIERKEY
df_sup_nation = df_supplier.merge(df_nation[df_nation['N_NAME'] == 'GERMANY'], left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Inner join of partsupp with the previous result on SUPPKEY
df_combined = df_partsupp.merge(df_sup_nation, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Compute the total value
df_combined['TOTAL_VALUE'] = df_combined['PS_SUPPLYCOST'] * df_combined['PS_AVAILQTY']

# Group by PARTKEY and calculate the sum of total values
df_grouped = df_combined.groupby('PS_PARTKEY', as_index=False).agg(VALUE=('TOTAL_VALUE', 'sum'))

# Apply the having condition by calculating the total sum and multiplying by the threshold
total_sum = df_grouped['VALUE'].sum() * 0.0001000000
df_grouped = df_grouped[df_grouped['VALUE'] > total_sum]

# Sort the results by VALUE in descending order
df_grouped.sort_values(by='VALUE', ascending=False, inplace=True)

# Output to CSV
df_grouped.to_csv('query_output.csv', index=False)
