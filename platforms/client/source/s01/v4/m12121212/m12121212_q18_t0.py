# mongo_redis_query.py

import pymongo
import pandas as pd
import direct_redis
from io import StringIO

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_orders_col = mongo_db['orders']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379)

# Load dataframes
orders_df = pd.DataFrame(list(mongo_orders_col.find({}, {'_id': 0})))
customer_df = pd.read_json(StringIO(redis_client.get('customer')))
lineitem_df = pd.read_json(StringIO(redis_client.get('lineitem')))

# Filter the lineitem table for the records with the sum of L_QUANTITY greater than 300
lineitem_sum_qty_df = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_QUANTITY'].sum() > 300)

# Perform the join operation
result_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result_df = pd.merge(result_df, lineitem_sum_qty_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by the necessary fields and calculate SUM(L_QUANTITY)
grouped_df = result_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])
summed_df = grouped_df['L_QUANTITY'].sum().reset_index(name='SUM_L_QUANTITY')

# Sort as specified
sorted_df = summed_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV
sorted_df.to_csv('query_output.csv', index=False)
