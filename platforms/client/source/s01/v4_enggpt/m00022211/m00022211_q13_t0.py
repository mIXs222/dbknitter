# import necessary libraries
import pymongo
from pymongo import MongoClient
import pandas as pd
import direct_redis

# Functions to retrieve data from MongoDB and Redis
def get_mongo_data():
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    orders_collection = db['orders']
    # exclude orders with comments containing 'pending' or 'deposits'
    query = {
        'O_COMMENT': {
            '$not': {
                '$regex': 'pending|deposits',
                '$options': 'i' # case-insensitive
            }
        }
    }
    # Project specific fields to match SQL-like query structure
    projection = {
        '_id': False,
        'O_ORDERKEY': True,
        'O_CUSTKEY': True,
        'O_COMMENT': True
    }
    orders_data = list(orders_collection.find(query, projection))
    return pd.DataFrame(orders_data)

def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    customer_data_str = r.get('customer')
    customer_data = pd.read_json(customer_data_str)
    return customer_data

# Retrieve data from databases
orders_df = get_mongo_data()
customers_df = get_redis_data()

# Merge data from Mongo and Redis
merged_df = pd.merge(customers_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Create a subquery to count orders that don't have 'pending' or 'deposits' in comments
subquery = merged_df.groupby('C_CUSTKEY').apply(lambda df: df['O_ORDERKEY'].count()).reset_index(name='C_COUNT')

# Join the subquery back with customers to get counts including zeros
result = pd.merge(customers_df, subquery, on='C_CUSTKEY', how='left')

# Get the distribution of customers by order count 'C_COUNT'
# Count unique customers for each 'C_COUNT'
cust_dist = result['C_COUNT'].value_counts().reset_index()
cust_dist.columns = ['C_COUNT', 'CUSTDIST']

# Sort results according to the instructions
final_result = cust_dist.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Save results to a CSV file
final_result.to_csv('query_output.csv', index=False)
