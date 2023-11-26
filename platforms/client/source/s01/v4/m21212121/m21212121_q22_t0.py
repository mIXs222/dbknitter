import pymongo
import pandas as pd
from bson.regex import Regex
from direct_redis import DirectRedis

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongodb = mongo_client['tpch']
customer_collection = mongodb['customer']

# Redis connection setup
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve average account balance from MongoDB where conditions are met
pipeline_avg_balance = [
    {'$match': {
        'C_ACCTBAL': {'$gt': 0},
        'C_PHONE': {'$regex': Regex('^(20|40|22|30|39|42|21)')}
    }},
    {'$group': {
        '_id': None,
        'avg_acctbal': {'$avg': '$C_ACCTBAL'}
    }}
]
avg_acctbal_result = list(customer_collection.aggregate(pipeline_avg_balance))[0]['avg_acctbal']

# Retrieve customers from MongoDB where conditions are met
pipeline_customers = [
    {'$match': {
        'C_ACCTBAL': {'$gt': avg_acctbal_result},
        'C_PHONE': {'$regex': Regex('^(20|40|22|30|39|42|21)')}
    }},
    {'$project': {
        '_id': 0,
        'C_CUSTKEY': 1,
        'CNTRYCODE': {'$substr': ['$C_PHONE', 0, 2]},
        'C_ACCTBAL': 1
    }}
]

customers_df = pd.DataFrame(list(customer_collection.aggregate(pipeline_customers)))

# Retrieve orders from Redis and convert to DataFrame
orders_df = pd.read_msgpack(redis_client.get('orders'))

# Merge data and perform LEFT ANTI JOIN to find customers without orders
merged_df = customers_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left', indicator=True)
merged_df = merged_df[merged_df['_merge'] == 'left_only']
del merged_df['_merge']

# Group by CNTRYCODE and calculate the required values
result_df = merged_df.groupby('CNTRYCODE')['C_ACCTBAL'].agg(NUMCUST='count', TOTACCTBAL='sum').reset_index()

# Order the results according to CNTRYCODE
result_df.sort_values(by=['CNTRYCODE'], inplace=True)

# Write the output to query_output.csv
result_df.to_csv('query_output.csv', index=False)
