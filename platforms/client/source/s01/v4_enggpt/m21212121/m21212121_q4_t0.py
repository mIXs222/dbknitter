import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis
import csv

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_lineitem_collection = mongo_db["lineitem"]

# Redis connection setup
redis_client = DirectRedis(host='redis', port=6379, db=0)

def get_orders_from_redis():
    orders_str = redis_client.get('orders')
    if orders_str:
        orders_df = pd.read_json(orders_str, orient='records')
        return orders_df
    else:
        return pd.DataFrame()

def get_lineitems_from_mongo():
    query = {
        "L_COMMITDATE": {"$lt": "L_RECEIPTDATE"},
        "L_SHIPDATE": {"$gte": datetime(1993, 7, 1)},
        "L_SHIPDATE": {"$lte": datetime(1993, 10, 1)}
    }
    projection = {"L_ORDERKEY": 1}
    lineitems_cursor = mongo_lineitem_collection.find(query, projection)
    lineitems_df = pd.DataFrame(list(lineitems_cursor))
    return lineitems_df

def main():
    # Retrieve data from Redis and MongoDB
    orders_df = get_orders_from_redis()
    lineitems_df = get_lineitems_from_mongo()

    # Join the dataframes on order key
    result_df = pd.merge(orders_df, lineitems_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

    # Perform the analysis based on the conditions and group by order priority
    final_df = result_df.groupby('O_ORDERPRIORITY').size().reset_index(name='order_count')

    # Sorting the final result as per the order priority
    final_df.sort_values('O_ORDERPRIORITY', inplace=True)

    # Write the final dataframe to CSV
    final_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
