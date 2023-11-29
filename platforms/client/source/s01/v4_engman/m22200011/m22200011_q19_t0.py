from pymongo import MongoClient
import pandas as pd
import csv
from direct_redis import DirectRedis

# MongoDB connection and query
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']
lineitem_collection = mongodb['lineitem']

lineitem_cursor = lineitem_collection.find({
    'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
    'L_SHIPINSTRUCT': 'DELIVER IN PERSON',
    '$or': [
        {
            'L_QUANTITY': {'$gte': 1, '$lte': 11},
            'P_PARTKEY': {'$in': query_part_keys_brand_12}
        },
        {
            'L_QUANTITY': {'$gte': 10, '$lte': 20},
            'P_PARTKEY': {'$in': query_part_keys_brand_23}
        },
        {
            'L_QUANTITY': {'$gte': 20, '$lte': 30},
            'P_PARTKEY': {'$in': query_part_keys_brand_34}
        }]
})

lineitem_df = pd.DataFrame(lineitem_cursor)
lineitem_df['REVENUE'] = lineitem_df.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)
revenue_sum = lineitem_df['REVENUE'].sum()

# Redis connection and query
redis_client = DirectRedis(host='redis', port=6379, db=0)

parts_brand_12 = redis_client.get('part:12:SM CASE,SM BOX,SM PACK,SM PKG')
parts_brand_23 = redis_client.get('part:23:MED BAG,MED BOX,MED PKG,MED PACK')
parts_brand_34 = redis_client.get('part:34:LG CASE,LG BOX,LG PACK,LG PKG')

query_part_keys_brand_12 = [p['P_PARTKEY'] for p in parts_brand_12 if 1 <= p['P_SIZE'] <= 5]
query_part_keys_brand_23 = [p['P_PARTKEY'] for p in parts_brand_23 if 1 <= p['P_SIZE'] <= 10]
query_part_keys_brand_34 = [p['P_PARTKEY'] for p in parts_brand_34 if 1 <= p['P_SIZE'] <= 15]

# Writing results to query_output.csv
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['REVENUE'])
    writer.writerow([revenue_sum])

# Close connections
client.close()
