# query_code.py
import pymongo
import redis
import pandas as pd
from pandas.io.json import json_normalize

# MongoDB connection and extraction
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
supplier = json_normalize(list(mongo_db.supplier.find({}, {'_id': 0})))
partsupp = json_normalize(list(mongo_db.partsupp.find({}, {'_id': 0})))

# Joining partsupp with supplier
parts_n_suppliers = pd.merge(partsupp, supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Redis connection and extraction
redis_client = redis.StrictRedis(host='redis', port=6379, db=0)
nation_str = redis_client.get('nation')
nation = pd.read_json(nation_str, lines=True)

# Joining previous dataframe with nation
merged = pd.merge(parts_n_suppliers, nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filtering by nation 'GERMANY'
germany_data = merged[merged['N_NAME'] == 'GERMANY']

# Computing VALUE and aggregating
grp = germany_data.groupby('PS_PARTKEY')
result = grp.apply(lambda df: pd.Series({
    'VALUE': (df['PS_SUPPLYCOST'] * df['PS_AVAILQTY']).sum()
})).reset_index()

# Subquery equivalent - calculating the threshold
threshold = germany_data['PS_SUPPLYCOST'].dot(germany_data['PS_AVAILQTY']) * 0.0001000000

# Filtering groups having VALUE greater than the threshold
final_result = result[result['VALUE'] > threshold]

# Sorting the result
final_result_sorted = final_result.sort_values('VALUE', ascending=False)

# Writing the result to a CSV file
final_result_sorted.to_csv('query_output.csv', index=False)
