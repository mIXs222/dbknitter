# Python code to run the query across different databases
import pymongo
import pandas as pd
from datetime import datetime
import redis
import csv

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_customers = mongo_db["customer"]
mongo_lineitem = mongo_db["lineitem"]

# Redis connection setup
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# MongoDB query for customers and lineitems
start_date = datetime(1993, 10, 1)
end_date = datetime(1994, 1, 1)

cust_pipeline = [
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'C_CUSTKEY',
            'foreignField': 'L_ORDERKEY', 
            'as': 'lineitems'
        },
    },
    {
        '$unwind': '$lineitems'
    },
    {
        '$match': {
            'lineitems.L_SHIPDATE': {
                '$gte': start_date,
                '$lte': end_date
            },
            'lineitems.L_RETURNFLAG': 'R'
        }
    },
    {
        '$project': {
            'C_CUSTKEY': True,
            'C_NAME': True,
            'C_ACCTBAL': True,
            'C_ADDRESS': True,
            'C_PHONE': True,
            'C_NATIONKEY': True,
            'C_COMMENT': True,
            'revenue_lost': {
                '$multiply': [
                    '$lineitems.L_EXTENDEDPRICE', {'$subtract': [1, '$lineitems.L_DISCOUNT']}
                ]
            }
        }
    }
]

mongo_customers_result = mongo_customers.aggregate(cust_pipeline)

# Convert Mongo result to pandas DataFrame
df_customers = pd.DataFrame(list(mongo_customers_result))

# Retrieve nation from Redis, and join with customers DataFrame
nation_data = redis_client.get('nation')
df_nation = pd.read_json(nation_data)

df_customers['C_NATIONKEY'] = df_customers['C_NATIONKEY'].astype(str)
df_nation['N_NATIONKEY'] = df_nation['N_NATIONKEY'].astype(str)

df_customers_with_nation = df_customers.merge(df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate the total revenue lost, sort and select columns for output
df_customers_with_nation['revenue_lost'] = df_customers_with_nation.groupby('C_CUSTKEY')['revenue_lost'].transform('sum')
df_output = df_customers_with_nation.drop_duplicates('C_CUSTKEY')

df_output = df_output.sort_values(by=['revenue_lost', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
                                  ascending=[True, True, True, False])

df_output = df_output[['C_CUSTKEY', 'C_NAME', 'revenue_lost', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]

# Write the output to a CSV file
df_output.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mongo_client.close()
