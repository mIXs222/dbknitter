import pymongo
import json
import csv
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation_collection = mongo_db["nation"]
supplier_collection = mongo_db["supplier"]

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Fetch SAUDI ARABIA nation key
saudi_arabia_key = nation_collection.find_one({"N_NAME": "SAUDI ARABIA"})["N_NATIONKEY"]

# Fetch suppliers from SAUDI ARABIA
suppliers = list(supplier_collection.find({"S_NATIONKEY": saudi_arabia_key}))

# Get orders and lineitem from Redis as DataFrames
orders = pd.DataFrame(json.loads(r.get('orders')))
lineitem = pd.DataFrame(json.loads(r.get('lineitem')))

# Filter orders with status 'F'
orders = orders[orders['O_ORDERSTATUS'] == 'F']

# Merge and filter orders with multiple suppliers
multi_supplier_orders = lineitem[lineitem.duplicated(subset=['L_ORDERKEY'], keep=False)]

# Merge suppliers and their lineitems
supplier_lineitem = pd.merge(suppliers, multi_supplier_orders, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Filter lineitems where the committed date is not met
failed_lineitems = supplier_lineitem[supplier_lineitem['L_COMMITDATE'] < supplier_lineitem['L_RECEIPTDATE']]

# Count the number of awaiting lineitems for each supplier
supplier_await_count = failed_lineitems.groupby(['S_SUPPKEY', 'S_NAME']).size().reset_index(name='NUMWAIT')

# Sort as per the query requirement
supplier_await_count_sorted = supplier_await_count.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Output to CSV file
supplier_await_count_sorted.to_csv('query_output.csv', index=False, columns=['NUMWAIT', 'S_NAME'])
