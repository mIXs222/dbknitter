import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
cust_coll = mongodb["customer"]

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MongoDB
customers_df = pd.DataFrame(list(cust_coll.find()))

# Retrieve data from Redis and convert to DataFrame
orders_df = pd.read_msgpack(redis_client.get('orders'))
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))

# Convert string columns to numeric values for proper aggregation in pandas
lineitem_df['L_QUANTITY'] = pd.to_numeric(lineitem_df['L_QUANTITY'], errors='coerce')
orders_df['O_TOTALPRICE'] = pd.to_numeric(orders_df['O_TOTALPRICE'], errors='coerce')

# Filter line items with total quantity greater than 300
lineitem_grouped = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_QUANTITY'].sum() > 300)

# Merge data frames to mimic SQL joins
merged_df = customers_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(lineitem_grouped, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Perform group by and order by operations
result_df = merged_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({'L_QUANTITY': 'sum'}).reset_index()
result_df = result_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Output result to a CSV file
result_df.to_csv('query_output.csv', index=False)
