# Python code to execute the query across multiple databases and write output to a CSV file
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB connection and query
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_supplier = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0})))
mongo_nation = pd.DataFrame(list(mongo_db.nation.find({}, {'_id': 0})))

# Filter suppliers based on nation 'CANADA'
canadian_suppliers = mongo_nation[mongo_nation['N_NAME'] == 'CANADA']['N_NATIONKEY'].tolist()
suppliers_in_canada = mongo_supplier[mongo_supplier['S_NATIONKEY'].isin(canadian_suppliers)]

# Redis connection and query
redis_client = DirectRedis(host='redis', port=6379, db=0)
partsupp_df = pd.read_msgpack(redis_client.get('partsupp'))
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))

# Filter partsupp where part name starts with 'forest'
partsupp_df_filtered = partsupp_df[partsupp_df['PS_PARTKEY'].str.startswith('forest')]

# Calculate the threshold quantity for line items
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= datetime(1994, 1, 1)) &
                                   (lineitem_df['L_SHIPDATE'] <= datetime(1995, 1, 1))]
grouped_lineitem = filtered_lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()
grouped_lineitem['threshold'] = grouped_lineitem['L_QUANTITY'] / 2

# Combine data from Redis to MongoDB data
common_supplier_keys = partsupp_df_filtered['PS_SUPPKEY'].unique()
qualified_suppliers = suppliers_in_canada[suppliers_in_canada['S_SUPPKEY'].isin(common_supplier_keys)]
qualified_suppliers = qualified_suppliers.merge(grouped_lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Final selection and sorting
final_selection = qualified_suppliers[['S_NAME', 'S_ADDRESS']].sort_values(by='S_NAME')
final_selection.to_csv('query_output.csv', index=False)
