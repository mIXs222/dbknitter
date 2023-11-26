import pymongo
import redis
import pandas as pd
from bson.json_util import dumps

# MongoDB Connection
client = pymongo.MongoClient("mongodb", 27017)
db = client["tpch"]

# Getting data from MongoDB
nation = pd.DataFrame(list(db.nation.find()))
supplier = pd.DataFrame(list(db.supplier.find()))

# Filter the nation for 'GERMANY'
nation_germany = nation[nation['N_NAME'] == 'GERMANY']

# Merge supplier and nation_germany
supplier_germany = pd.merge(supplier, nation_germany, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Redis Connection
r = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
# Workaround for the custom DirectRedis class not provided
# Assuming DirectRedis gets pandas DataFrame directly from Redis
class DirectRedis(redis.StrictRedis):
    def get(self, name):
        value = super().get(name)
        return pd.read_json(value) if value else pd.DataFrame()

r = DirectRedis(host='redis', port=6379, db=0, decode_responses=True)

# Getting data from Redis
partsupp = r.get('partsupp')
# Converting JSON string to DataFrame
partsupp = pd.read_json(partsupp)

# Larger dataset - JOIN Operation Replacement
merged_data = pd.merge(partsupp, supplier_germany, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculation
merged_data['VALUE'] = merged_data['PS_SUPPLYCOST'] * merged_data['PS_AVAILQTY']
group_data = merged_data.groupby('PS_PARTKEY').agg({'VALUE': 'sum'}).reset_index()

# Sub-query equivalent calculation
total_value = merged_data['VALUE'].sum() * 0.0001000000

# Having clause replacement
group_data = group_data[group_data['VALUE'] > total_value]

# Order the results
group_data.sort_values('VALUE', ascending=False, inplace=True)

# Write the results to CSV
group_data.to_csv('query_output.csv', index=False)
