# query_executer.py
from pymongo import MongoClient
import direct_redis
import pandas as pd

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Connect to Redis
redis = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'orders' collection from MongoDB and filter out pending and deposits
mongo_orders_query = {
    "$and": [
        {"O_COMMENT": {"$not": {"$regex": ".*pending.*deposits.*"}}},
        {"O_ORDERSTATUS": {"$ne": "O"}}
    ]
}
projection_fields = {
    "_id": 0, "O_CUSTKEY": 1
}
orders_data = list(orders_collection.find(mongo_orders_query, projection_fields))
orders_df = pd.DataFrame(orders_data)

# Retrieve 'customer' table from Redis and load into Pandas DataFrame
customer_data = redis.get('customer')
customer_df = pd.read_json(customer_data)

# Merge the two datasets on C_CUSTKEY and O_CUSTKEY
merged_df = pd.merge(orders_df, customer_df, how='inner',
                     left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Create the output dataframe with counts of orders per customer
output_df = pd.DataFrame({'num_orders': merged_df.groupby('C_CUSTKEY').size()})
output_df = output_df['num_orders'].value_counts().reset_index()
output_df.columns = ['num_orders', 'num_customers']

# Write the results to 'query_output.csv'
output_df.to_csv('query_output.csv', index=False)
