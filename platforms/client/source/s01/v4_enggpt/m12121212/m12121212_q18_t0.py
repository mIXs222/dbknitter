import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Retrieve orders from MongoDB
orders_df = pd.DataFrame(list(orders_collection.find({}, {'_id': 0})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve customers and lineitems from Redis
customers_df = pd.read_json(redis_client.get('customer').decode('utf-8'))
lineitems_df = pd.read_json(redis_client.get('lineitem').decode('utf-8'))

# Calculate total quantity per order
lineitems_sum_df = lineitems_df.groupby('L_ORDERKEY')['L_QUANTITY'].sum().reset_index()
lineitems_sum_df.rename(columns={'L_QUANTITY': 'TOTAL_QUANTITY'}, inplace=True)
orders_over_300 = lineitems_sum_df[lineitems_sum_df['TOTAL_QUANTITY'] > 300]

# Merge orders_over_300 with orders on order key
orders_df = orders_df.merge(orders_over_300, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Merge orders with customers
result_df = pd.merge(customers_df, orders_df, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Merge result with lineitems to get the sum of quantities
result_df = result_df.merge(lineitems_df[['L_ORDERKEY', 'L_QUANTITY']], how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Grouping and ordering the result
final_df = result_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])\
                    .agg({'L_QUANTITY': 'sum'})\
                    .reset_index()\
                    .rename(columns={'L_QUANTITY': 'SUM_QUANTITIES'})

final_df = final_df.sort_values(['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Writing the output to a CSV file
final_df.to_csv('query_output.csv', index=False)
