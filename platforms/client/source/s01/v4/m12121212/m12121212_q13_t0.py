import pandas as pd
from pymongo import MongoClient
import direct_redis
import os

# Function to connect to MongoDB
def connect_mongodb(host, port, db_name):
    client = MongoClient(host, port)
    db = client[db_name]
    return db

# Function to connect to Redis
def connect_redis(host, port, db_name):
    return direct_redis.DirectRedis(host=host, port=port, db=db_name)

# Function to run the aggregation query
def run_aggregation(mongo_db, redis_db):
    # Extract data from MongoDB (orders)
    orders = pd.DataFrame(list(mongo_db.orders.find({}, {
        '_id': 0,
        'O_ORDERKEY': 1,
        'O_CUSTKEY': 1,
        'O_COMMENT': 1
    })))

    # Exclude orders with comments containing 'pending%deposits%'
    orders_filtered = orders[~orders['O_COMMENT'].str.contains('pending%deposits%', regex=False)]

    # Extract data from Redis (customer)
    customer_data = redis_db.get('customer')
    customer = pd.read_json(customer_data, orient='records')

    # Left join orders and customer on C_CUSTKEY = O_CUSTKEY
    result = pd.merge(customer, orders_filtered, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

    # Perform aggregation
    grouped = result.groupby('C_CUSTKEY')['O_ORDERKEY'].count().reset_index(name='C_COUNT')
    cust_dist = grouped.groupby('C_COUNT').size().reset_index(name='CUSTDIST')

    # Sorting the results
    cust_dist.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)

    return cust_dist

def main():
    # Connect to MongoDB
    mongo_db = connect_mongodb(host='mongodb', port=27017, db_name='tpch')

    # Connect to Redis
    redis_db = connect_redis(host='redis', port=6379, db_name=0)

    # Run aggregation and get result
    result_df = run_aggregation(mongo_db, redis_db)

    # Write result to CSV
    result_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
