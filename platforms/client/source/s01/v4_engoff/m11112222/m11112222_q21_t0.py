# query_supplier_waiting.py

import pymongo
import redis
import pandas as pd

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation_collection = mongo_db["nation"]
supplier_collection = mongo_db["supplier"]

# Get the information for SAUDI ARABIA
saudi_arabia = nation_collection.find_one({'N_NAME': 'SAUDI ARABIA'}, {'_id': 0, 'N_NATIONKEY': 1})
saudi_nationkey = saudi_arabia['N_NATIONKEY']

# Get suppliers from SAUDI ARABIA
saudi_suppliers_cursor = supplier_collection.find({'S_NATIONKEY': saudi_nationkey}, {'_id': 0})
saudi_suppliers = list(saudi_suppliers_cursor)

# Convert to pandas dataframe
saudi_suppliers_df = pd.DataFrame(saudi_suppliers)

# Redis Connection
r = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Convert JSON from Redis to Pandas DataFrame
orders_json = r.get('orders')
lineitem_json = r.get('lineitem')

orders_df = pd.read_json(orders_json if orders_json else '[]')
lineitem_df = pd.read_json(lineitem_json if lineitem_json else '[]')

# Filter orders with status 'F'
failed_orders_df = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

# Find suppliers who are solely responsible for the delayed orders
def check_supplier_responsibility(row, lineitem_data):
    order_lineitems = lineitem_data[lineitem_data['L_ORDERKEY'] == row['O_ORDERKEY']]
    delayed_lineitems = order_lineitems[order_lineitems['L_RECEIPTDATE'] > order_lineitems['L_COMMITDATE']]
    unique_suppliers_in_delay = delayed_lineitems['L_SUPPKEY'].unique()
    return (len(unique_suppliers_in_delay) == 1) and (unique_suppliers_in_delay[0] == row['S_SUPPKEY'])

saudi_suppliers_df['DELAY'] = saudi_suppliers_df.apply(lambda row: check_supplier_responsibility(row, lineitem_df), axis=1)

# Filter suppliers who kept orders waiting
suppliers_kept_waiting_df = saudi_suppliers_df[saudi_suppliers_df['DELAY']]

# Output results to a CSV file
suppliers_kept_waiting_df.to_csv('query_output.csv', index=False)
