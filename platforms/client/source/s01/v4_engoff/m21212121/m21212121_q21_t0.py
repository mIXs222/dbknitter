import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client['tpch']
supplier_col = mongo_db['supplier']
lineitem_col = mongo_db['lineitem']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_client.get('nation'), orient='records')
orders_df = pd.read_json(redis_client.get('orders'), orient='records')

# Query data from MongoDB for supplier and lineitem
suppliers = pd.DataFrame(list(supplier_col.find()))
lineitems = pd.DataFrame(list(lineitem_col.find()))

# Extract relevant information from nation and orders tables in Redis
nation_df = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']
orders_df = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

# Merge tables to prepare for final query
lineitem_orders = pd.merge(lineitems, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
lineitem_orders_suppliers = pd.merge(lineitem_orders, suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Perform the query with the current date to evaluate the 'kept waiting' condition
current_date = datetime.now().strftime('%Y-%m-%d')
result = lineitem_orders_suppliers[
    (lineitem_orders_suppliers['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])) &
    (lineitem_orders_suppliers['L_COMMITDATE'] < lineitem_orders_suppliers['L_RECEIPTDATE']) &
    (lineitem_orders_suppliers['L_RECEIPTDATE'] > current_date)
]['S_NAME'].drop_duplicates()

# Write the result to a CSV file
result.to_csv('query_output.csv', index=False)
