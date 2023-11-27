import pymongo
import pandas as pd
from pymongo import MongoClient
import direct_redis

# MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Fetching data from MongoDB
nation = pd.DataFrame(list(db.nation.find({}, {'_id': 0})))
partsupp = pd.DataFrame(list(db.partsupp.find({}, {'_id': 0})))

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch supplier table stored as a pickled DataFrame in Redis
supplier_df_data = r.get('supplier')
supplier = pd.read_pickle(supplier_df_data)

# Filtering for German suppliers
german_nation_keys = nation[nation['N_NAME'] == 'GERMANY']['N_NATIONKEY'].tolist()
german_suppliers = supplier[supplier['S_NATIONKEY'].isin(german_nation_keys)]

# Merging the partsupp and german_suppliers dataframes
combined_df = pd.merge(partsupp, german_suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Applying the SQL logic to the combined dataframe
combined_df['VALUE'] = combined_df['PS_SUPPLYCOST'] * combined_df['PS_AVAILQTY']
result = combined_df.groupby('PS_PARTKEY')['VALUE'].sum().reset_index()

# Subquery to create the value that will be used in the HAVING clause
sub_query_value = combined_df['VALUE'].sum() * 0.0001000000

# Filtering using the subquery result
final_result = result[result['VALUE'] > sub_query_value]

# Sorting the final result
final_result = final_result.sort_values(by='VALUE', ascending=False)

# Writing the result to a CSV file
final_result.to_csv('query_output.csv', index=False)
