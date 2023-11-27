# mongo_redis_query.py
import pymongo
import pandas as pd
from decimal import Decimal
import direct_redis

# MongoDB connection and query
def mongodb_query(mongo_client, brand_ids, sizes, containers, quantities):
    db = mongo_client.tpch
    lineitem = pd.DataFrame(list(db.lineitem.find({
        'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
        'L_SHIPINSTRUCT': 'DELIVER IN PERSON',
        'L_PARTKEY': {'$in': brand_ids},
        'L_QUANTITY': {'$gte': quantities[0], '$lte': quantities[1]},
        'L_EXTENDEDPRICE': {'$exists': True},
        'L_DISCOUNT': {'$exists': True}
    })))
    return lineitem

# Redis connection and query
def redis_query(redis_client, brand_id, size_range, container_list):
    part_keys = []
    for size in range(size_range[0], size_range[1] + 1):
        for container in container_list:
            part_keys.extend(redis_client.get(f"{brand_id}-{size}-{container}"))

    return part_keys

def main():
    # MongoDB Connection
    mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
    
    # Redis Connection
    redis_client = direct_redis.DirectRedis(host='redis', port=6379)

    # Parts define based on query details
    parts = [
        {'brand_id': 12, 'size_range': (1, 5), 'containers': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'], 'quantities': (1, 11)},
        {'brand_id': 23, 'size_range': (1, 10), 'containers': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'], 'quantities': (10, 20)},
        {'brand_id': 34, 'size_range': (1, 15), 'containers': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'], 'quantities': (20, 30)},
    ]

    all_results = pd.DataFrame()

    for part in parts:
        brand_keys = redis_query(redis_client, part['brand_id'], part['size_range'], part['containers'])
        mongodb_results = mongodb_query(mongo_client, brand_keys, part['size_range'], part['containers'], part['quantities'])
        all_results = all_results.append(mongodb_results, ignore_index=True)

    # Calculate the discounted price
    all_results['DISCOUNT_PRICE'] = all_results.apply(lambda row: (row['L_EXTENDEDPRICE'] * (Decimal(1) - row['L_DISCOUNT'])), axis=1)

    # Store the results
    all_results.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
