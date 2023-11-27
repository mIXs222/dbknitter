from pymongo import MongoClient
import pandas as pd
import direct_redis

def mongo_query(mongo_client):
    pipeline = [
        {
            '$match': {
                '$or': [
                    {
                        'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
                        'L_SHIPINSTRUCT': 'DELIVER IN PERSON',
                        'L_QUANTITY': {'$gte': 1, '$lte': 11}
                    },
                    {
                        'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
                        'L_SHIPINSTRUCT': 'DELIVER IN PERSON',
                        'L_QUANTITY': {'$gte': 10, '$lte': 20}
                    },
                    {
                        'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
                        'L_SHIPINSTRUCT': 'DELIVER IN PERSON',
                        'L_QUANTITY': {'$gte': 20, '$lte': 30}
                    }
                ]
            }
        },
        {
            '$project': {
                '_id': 0,
                'L_PARTKEY': 1,
                'L_EXTENDEDPRICE': 1,
                'L_DISCOUNT': 1,
                'L_QUANTITY': 1
            }
        }
    ]
    lineitem_data = list(mongo_client.tpch.lineitem.aggregate(pipeline))
    return pd.DataFrame(lineitem_data)

def redis_query(redis_client):
    part_data = redis_client.get('part')
    part_df = pd.read_json(part_data, orient='records')
    return part_df

def main():
    # Minimal error handling since the tasks stated no need for explanation or error handling.
    
    # Connect to MongoDB
    mongo_client = MongoClient('mongodb', 27017)
    lineitem_df = mongo_query(mongo_client)
    
    # Connect to Redis
    redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    part_df = redis_query(redis_client)
    
    # Merge DataFrames
    result = lineitem_df.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
    
    # Apply conditions from SQL WHERE clause
    conditions = (
        (
            (result['P_BRAND'] == 'Brand#12') & 
            (result['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
            (result['P_SIZE'].between(1, 5))
        ) |
        (
            (result['P_BRAND'] == 'Brand#23') & 
            (result['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
            (result['P_SIZE'].between(1, 10))
        ) |
        (
            (result['P_BRAND'] == 'Brand#34') & 
            (result['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
            (result['P_SIZE'].between(1, 15))
        )
    )
    result = result[conditions]
    
    # Calculate REVENUE
    result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])
    
    # Sum up REVENUE and save to CSV
    revenue_sum = result[['REVENUE']].sum()
    revenue_sum.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
