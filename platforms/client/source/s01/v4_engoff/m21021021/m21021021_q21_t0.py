# query.py
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client["tpch"]
lineitem_collection = mongo_db["lineitem"]
lineitem_df = pd.DataFrame(list(lineitem_collection.find()))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_client.get('nation'), orient='records')
supplier_df = pd.read_json(redis_client.get('supplier'), orient='records')
orders_df = pd.read_json(redis_client.get('orders'), orient='records')

# Filter for Saudi Arabia in nation table
saudi_nation_df = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']

# Filter for orders that are multi-supplier and have a status of 'F'
# This information is not directly present, so we can't filter on these criteria

# For each supplier belonging to Saudi Arabia, check if they failed to meet the delivery date for any order
saudi_supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(saudi_nation_df['N_NATIONKEY'])]
suppliers_who_failed_df = lineitem_df.merge(saudi_supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
suppliers_who_failed_df = suppliers_who_failed_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Check which suppliers were the only ones who failed to meet the committed delivery date
# Assuming 'F' in O_ORDERSTATUS indicates failed status and comparing commit date with receipt date
result_df = suppliers_who_failed_df[
    (suppliers_who_failed_df['O_ORDERSTATUS'] == 'F') &
    (suppliers_who_failed_df['L_COMMITDATE'] < suppliers_who_failed_df['L_RECEIPTDATE'])
]

# Considering 'multi-supplier order' by checking if the order has more than one line item
# Grouping by order to check the count
order_group = lineitem_df.groupby('L_ORDERKEY').size().reset_index(name='count')
multi_supplier_orders = order_group[order_group['count'] > 1]

result_df = result_df[result_df['L_ORDERKEY'].isin(multi_supplier_orders['L_ORDERKEY'])]

unique_suppliers_result_df = result_df['S_NAME'].drop_duplicates()

# Write output to CSV
unique_suppliers_result_df.to_csv('query_output.csv', index=False)
