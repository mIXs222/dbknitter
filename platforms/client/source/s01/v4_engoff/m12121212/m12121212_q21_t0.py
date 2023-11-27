from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd

# Connect to the MongoDB instance
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Retrieve the nation and orders data from MongoDB
nation_df = pd.DataFrame(list(mongo_db.nation.find({}, {'_id': 0})))
orders_df = pd.DataFrame(list(mongo_db.orders.find({}, {'_id': 0})))

# Filter nations for 'SAUDI ARABIA'
saudi_nations = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve the supplier and lineitem data from Redis
supplier_df = pd.read_json(redis_client.get('supplier'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Join orders with lineitems on order key where order status is 'F'
failed_orders = orders_df[orders_df['O_ORDERSTATUS'] == 'F']
failed_lineitems = lineitem_df[lineitem_df['L_ORDERKEY'].isin(failed_orders['O_ORDERKEY'])]

# Identify all suppliers associated with failed orders
supplier_lineitems = failed_lineitems.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filter suppliers by nation key of 'SAUDI ARABIA'
saudi_suppliers = supplier_lineitems[supplier_lineitems['S_NATIONKEY'].isin(saudi_nations['N_NATIONKEY'])]

# Select suppliers who were the only one that failed to meet delivery date for an order
agg_data = supplier_lineitems.groupby('L_ORDERKEY').agg({'L_RECEIPTDATE': 'max', 'L_COMMITDATE': 'max'})
late_deliveries = agg_data[agg_data['L_RECEIPTDATE'] > agg_data['L_COMMITDATE']].index
result = saudi_suppliers[saudi_suppliers['L_ORDERKEY'].isin(late_deliveries)]

# Drop duplicates to find unique suppliers and select relevant columns
unique_suppliers = result.drop_duplicates(subset=['S_SUPPKEY'])[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']]

# Write the result to a CSV file
unique_suppliers.to_csv('query_output.csv', index=False)
