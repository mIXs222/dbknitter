# import necessary libraries
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to mongodb
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongodb_client['tpch']

# Load mongodb tables
nation_df = pd.DataFrame(list(mongo_db['nation'].find({}, {'_id': 0})))
part_df = pd.DataFrame(list(mongo_db['part'].find({}, {'_id': 0})))
partsupp_df = pd.DataFrame(list(mongo_db['partsupp'].find({}, {'_id': 0})))

# Filter parts provided by Canadian suppliers
canadian_nations = nation_df[nation_df['N_NAME'] == 'CANADA']
supplier_keys = canadian_nations['N_NATIONKEY'].values.tolist()

# Connect to redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve redis tables as pandas dataframes
supplier_df = pd.read_json(redis_client.get('supplier'))
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(supplier_keys)]

lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Additional filters based on part name and ship date
parts_forest = part_df[part_df['P_NAME'].str.startswith('forest')]['P_PARTKEY'].values.tolist()
lineitem_filtered = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= datetime.strptime('1994-01-01', '%Y-%m-%d')) &
    (lineitem_df['L_SHIPDATE'] <= datetime.strptime('1995-01-01', '%Y-%m-%d')) &
    (lineitem_df['L_PARTKEY'].isin(parts_forest))
]

# Compute threshold quantity (50% of total line item quantity)
threshold_qtys = lineitem_filtered.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum() * 0.5
threshold_qtys_df = threshold_qtys.reset_index()
threshold_qtys_df.rename(columns={'L_QUANTITY': 'THRESHOLD_QUANTITY'}, inplace=True)

# Match suppliers and threshold quantities
partsupp_filtered = partsupp_df[
    (partsupp_df['PS_PARTKEY'].isin(parts_forest)) &
    (partsupp_df['PS_AVAILQTY'] >= threshold_qtys_df['THRESHOLD_QUANTITY'])
]

# Combine the results to get the final output
final_df = supplier_df[supplier_df['S_SUPPKEY'].isin(partsupp_filtered['PS_SUPPKEY'])][['S_NAME', 'S_ADDRESS']]

# Sort by supplier name and write out to file
final_df.sort_values('S_NAME').to_csv('query_output.csv', index=False)
