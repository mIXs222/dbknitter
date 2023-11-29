# mongo_query.py

from pymongo import MongoClient
import direct_redis
import pandas as pd

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Read collections from MongoDB
customer_df = pd.DataFrame(list(mongo_db.customer.find()))
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find()))

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read data from Redis
orders_df = pd.read_json(r.get('orders'), orient='records')

# Combine the data into a single DataFrame
joined_df = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')\
               .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter large orders
large_orders_df = joined_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])\
                           .sum().reset_index()
large_orders_df = large_orders_df[large_orders_df['L_QUANTITY'] > 300]

# Selecting the required columns
result_df = large_orders_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Sort the result by O_TOTALPRICE in descending and O_ORDERDATE in ascending order
result_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
