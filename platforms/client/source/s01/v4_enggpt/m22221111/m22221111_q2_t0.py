# import_packages.py
import pymongo
import pandas as pd
import direct_redis

# Set connection details
MONGODB_DETAILS = {
    'database': 'tpch',
    'port': 27017,
    'hostname': 'mongodb'
}

REDIS_DETAILS = {
    'database': 0,
    'port': 6379,
    'hostname': 'redis'
}

# Connect to MongoDB
client = pymongo.MongoClient(host=MONGODB_DETAILS['hostname'], port=MONGODB_DETAILS['port'])
mongo_db = client[MONGODB_DETAILS['database']]
partsupp_collection = mongo_db['partsupp']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host=REDIS_DETAILS['hostname'], port=REDIS_DETAILS['port'], db=REDIS_DETAILS['database'])

# Retrieve data from Redis
df_nation = pd.DataFrame(redis_client.get('nation'))
df_region = pd.DataFrame(redis_client.get('region'))
df_part = pd.DataFrame(redis_client.get('part'))
df_supplier = pd.DataFrame(redis_client.get('supplier'))

# Filter the Redis data based on the criteria
europe_region = df_region[df_region['R_NAME'] == 'EUROPE']
european_nations = df_nation[df_nation['N_REGIONKEY'].isin(europe_region['R_REGIONKEY'])]
european_suppliers = df_supplier[df_supplier['S_NATIONKEY'].isin(european_nations['N_NATIONKEY'])]

matching_parts = df_part[(df_part['P_SIZE'] == 15) & (df_part['P_TYPE'].str.contains('BRASS'))]

# Retrieve partsupp data from MongoDB and convert to DataFrame
partsupp = list(partsupp_collection.find())
df_partsupp = pd.DataFrame(partsupp)

# Perform the join operation to combine the datasets
combined_df = pd.merge(european_suppliers, df_partsupp, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
combined_df = pd.merge(combined_df, matching_parts, left_on='PS_PARTKEY', right_on='P_PARTKEY')
combined_df = pd.merge(combined_df, european_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter based on the minimum supply cost within the 'EUROPE' region
combined_df = combined_df[combined_df['PS_SUPPLYCOST'] == combined_df.groupby('S_NATIONKEY')['PS_SUPPLYCOST'].transform('min')]

# Select columns and order the results
final_df = combined_df[[
    'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT',
    'P_PARTKEY', 'P_MFGR', 'P_SIZE',
    'N_NAME'
]].sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write the final DataFrame to a CSV file
final_df.to_csv('query_output.csv', index=False)
