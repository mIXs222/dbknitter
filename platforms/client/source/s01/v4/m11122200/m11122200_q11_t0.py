import pymongo
import pandas as pd
import direct_redis

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_nation = pd.DataFrame(list(mongo_db["nation"].find()))

# Redis Connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
df_supplier = redis_client.get('supplier')
df_partsupp = redis_client.get('partsupp')

# Filter nation for 'GERMANY' and join with supplier
nation_germany = mongo_nation[mongo_nation['N_NAME'] == 'GERMANY']
supplier_germany = df_supplier[df_supplier['S_NATIONKEY'].isin(nation_germany['N_NATIONKEY'])]

# Join supplier with partsupp
ps_supplier = df_partsupp.merge(supplier_germany, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate SUM(PS_SUPPLYCOST * PS_AVAILQTY) for each PS_PARTKEY
ps_supplier['VALUE'] = ps_supplier['PS_SUPPLYCOST'] * ps_supplier['PS_AVAILQTY']
grouped = ps_supplier.groupby('PS_PARTKEY').agg({'VALUE': 'sum'}).reset_index()

# Calculate total value for all parts in Germany
total_value_germany = ps_supplier['VALUE'].sum() * 0.0001000000

# Filter groups having VALUE > total_value_germany
filtered_groups = grouped[grouped['VALUE'] > total_value_germany]

# Save the output in 'query_output.csv'
filtered_groups.sort_values(by='VALUE', ascending=False).to_csv('query_output.csv', index=False)
