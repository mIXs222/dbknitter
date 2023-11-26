# Python code to execute the combined query on MongoDB and Redis
import pandas as pd
from pymongo import MongoClient
from direct_redis import DirectRedis

# Function to connect to MongoDB
def connect_mongodb(hostname, port, dbname):
    client = MongoClient(host=hostname, port=port)
    db = client[dbname]
    return db

# Function to get data from MongoDB based on certain conditions
def get_mongodb_data(db, conditions):
    lineitem_data = pd.DataFrame(list(db.lineitem.find(conditions)))
    return lineitem_data

# Function to connect to Redis and get data
def get_redis_data(hostname, port, dbname, tablename):
    redis_client = DirectRedis(host=hostname, port=port, db=dbname)
    part_data = pd.read_json(redis_client.get(tablename))
    return part_data

# MongoDB connection details
mongodb_connection = {
    'hostname': 'mongodb',
    'port': 27017,
    'dbname': 'tpch'
}

# Redis connection details
redis_connection = {
    'hostname': 'redis',
    'port': 6379,
    'dbname': 0,
    'tablename': 'part'
}

# Connect to MongoDB
db = connect_mongodb(**mongodb_connection)

# MongoDB filter conditions
mongo_conditions = {
    '$or': [
        {
            'L_QUANTITY': {'$gte': 1, '$lte': 11},
            'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
            'L_SHIPINSTRUCT': 'DELIVER IN PERSON'
        },
        {
            'L_QUANTITY': {'$gte': 10, '$lte': 20},
            'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
            'L_SHIPINSTRUCT': 'DELIVER IN PERSON'
        },
        {
            'L_QUANTITY': {'$gte': 20, '$lte': 30},
            'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
            'L_SHIPINSTRUCT': 'DELIVER IN PERSON'
        }
    ]
}

# Get lineitem data from MongoDB
lineitem_data = get_mongodb_data(db, mongo_conditions)

# Connect to Redis and get part data
part_data = get_redis_data(**redis_connection)

# Perform join on P_PARTKEY and L_PARTKEY
merged_data = lineitem_data.merge(part_data, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the revenue
merged_data['REVENUE'] = merged_data.apply(
    lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']),
    axis=1
)

# Filter the final DataFrame
result = merged_data[
    (merged_data['P_BRAND'] == 'Brand#12') &
    (merged_data['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
    (merged_data['P_SIZE'] >= 1) & (merged_data['P_SIZE'] <= 5) |
    (merged_data['P_BRAND'] == 'Brand#23') &
    (merged_data['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
    (merged_data['P_SIZE'] >= 1) & (merged_data['P_SIZE'] <= 10) |
    (merged_data['P_BRAND'] == 'Brand#34') &
    (merged_data['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
    (merged_data['P_SIZE'] >= 1) & (merged_data['P_SIZE'] <= 15)
]

# Aggregate the revenue
final_result = result[['REVENUE']].sum().reset_index()
final_result.columns = ['_', 'REVENUE']

# Write the output to query_output.csv
final_result.to_csv('query_output.csv', index=False)
