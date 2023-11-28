import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv


def main():
    # Connect to MongoDB
    mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
    mongo_db = mongo_client["tpch"]
    orders_collection = mongo_db["orders"]

    # Connect to Redis
    redis_client = DirectRedis(host='redis', port=6379, db=0)

    # Load data from Redis
    customer_data = redis_client.get('customer')
    customers_df = pd.read_json(customer_data)

    # Load data from MongoDB
    orders_cursor = orders_collection.find(
        {
            'O_COMMENT': {'$not': {'$regex': 'pending|deposits'}}
        },
        {
            '_id': False,
            'O_ORDERKEY': True,
            'O_CUSTKEY': True,
            'O_COMMENT': True
        }
    )
    orders_df = pd.DataFrame(list(orders_cursor))

    # Perform the left outer join on 'C_CUSTKEY' and 'O_CUSTKEY'
    merged_df = pd.merge(customers_df, orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

    # Compute counts within a subquery
    merged_df['C_COUNT'] = merged_df.groupby('C_CUSTKEY')['O_ORDERKEY'].transform('count')

    # Perform aggregation to calculate 'CUSTDIST'
    result_df = merged_df.groupby('C_COUNT').agg(CUSTDIST=('C_CUSTKEY', 'nunique')).reset_index()

    # Sorting the result based on the requirements
    result_df = result_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

    # Write output to CSV
    result_df.to_csv('query_output.csv', index=False)


if __name__ == "__main__":
    main()
