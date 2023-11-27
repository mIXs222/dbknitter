import pymongo
import redis
import pandas as pd
from direct_redis import DirectRedis

# connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# get orders data from mongodb excluding comments with 'pending deposits'
orders_pipeline = [
    {
        '$match': {
            'O_COMMENT': {
                '$not': {
                    '$regex': 'pending.*deposits',
                    '$options': 'i'  # Case insensitive
                }
            }
        }
    },
    {
        '$project': {
            '_id': 0,
            'O_ORDERKEY': 1,
            'O_CUSTKEY': 1
        }
    }
]
orders_data = list(orders_collection.aggregate(orders_pipeline))
df_orders = pd.DataFrame(orders_data)

# connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# get customer data from redis
customer_data = redis_client.get('customer')
df_customers = pd.read_json(customer_data, orient='records')

# merge and perform the query
merged_df = pd.merge(
    df_customers,
    df_orders,
    how='left',
    left_on='C_CUSTKEY',
    right_on='O_CUSTKEY'
)

counted_df = merged_df.groupby('C_CUSTKEY').agg(C_COUNT=('O_ORDERKEY', 'count')).reset_index()
custdist_df = counted_df.groupby('C_COUNT').agg(CUSTDIST=('C_COUNT', 'count')).reset_index()

# sort as per the given query
custdist_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)

# write output to CSV
custdist_df.to_csv('query_output.csv', index=False)
