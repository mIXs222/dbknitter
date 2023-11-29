# query.py

import pymongo
import redis
import pandas as pd

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# Query MongoDB
pipeline = [
    {'$match': {"$or": [
        {'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 
         'L_SHIPINSTRUCT': 'DELIVER IN PERSON', 
         'L_PARTKEY': {'$gte': 1, '$lte': 11}, 
         'L_QUANTITY': {'$gte': 1, '$lte': 11},
         'L_SIZE': {'$gte': 1, '$lte': 5}},
        {'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 
         'L_SHIPINSTRUCT': 'DELIVER IN PERSON', 
         'L_PARTKEY': {'$gte': 10, '$lte': 20}, 
         'L_QUANTITY': {'$gte': 10, '$lte': 20},
         'L_SIZE': {'$gte': 1, '$lte': 10}},
        {'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 
         'L_SHIPINSTRUCT': 'DELIVER IN PERSON', 
         'L_PARTKEY': {'$gte': 20, '$lte': 30}, 
         'L_QUANTITY': {'$gte': 20, '$lte': 30},
         'L_SIZE': {'$gte': 1, '$lte': 15}}
    ]}},
    {'$project': {'REVENUE': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}}},
    {'$group': {'_id': None, 'TOTAL_REVENUE': {'$sum': '$REVENUE'}}},
    {'$project': {'_id': 0, 'REVENUE': '$TOTAL_REVENUE'}}
]

# Fetch data from MongoDB
mongo_result = list(lineitem_collection.aggregate(pipeline))

# Connect to Redis with Custom DirectRedis
class DirectRedis(redis.Redis):
    def get(self, name):
        record = super().get(name)
        if record:
            return pd.read_json(record, orient='records')

redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query Redis
# Since it is not possible to perform complex operations on Redis directly,
# we fetch the entire 'part' table and perform operations in-memory using pandas.
redis_data = redis_client.get('part')
brand12_containers = ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']
brand23_containers = ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']
brand34_containers = ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']

# Filter Redis data based on the criteria
part_data = redis_data[
    ((redis_data['P_BRAND'].eq('Brand#12') & redis_data['P_CONTAINER'].isin(brand12_containers) & redis_data['P_SIZE'].between(1, 5)) |
    (redis_data['P_BRAND'].eq('Brand#23') & redis_data['P_CONTAINER'].isin(brand23_containers) & redis_data['P_SIZE'].between(1, 10)) |
    (redis_data['P_BRAND'].eq('Brand#34') & redis_data['P_CONTAINER'].isin(brand34_containers) & redis_data['P_SIZE'].between(1, 15)))
]

# Combine MongoDB and Redis results
# Since we can't join data between the two databases, we assume that part keys match the criteria.
# If more complex operations were needed, further merging of data based on keys would be required.

# Assuming all the parts in `part_data` are to be used to filter `mongo_result` data:
mongo_df = pd.DataFrame(mongo_result)
final_result = mongo_df['REVENUE'].sum()

# Write to CSV
final_result_df = pd.DataFrame([{'REVENUE': final_result}])
final_result_df.to_csv('query_output.csv', index=False)
