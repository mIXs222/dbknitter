# Python code to execute the described query (query.py)

from pymongo import MongoClient
import direct_redis
import pandas as pd

def connect_to_mongodb(host, port, db_name):
    client = MongoClient(host, port)
    db = client[db_name]
    return db

def connect_to_redis(host, port, db_name):
    return direct_redis.DirectRedis(host=host, port=port, db=db_name)

def get_customers_from_mongodb(db):
    return pd.DataFrame(list(db.customer.find({}, {
        '_id': 0,
        'C_CUSTKEY': 1
    })))

def get_orders_from_redis(redis_connection):
    orders_df = redis_connection.get('orders')
    orders_df = pd.read_json(orders_df, orient='records')
    return orders_df[(~orders_df['O_COMMENT'].str.contains('pending')) &
                     (~orders_df['O_COMMENT'].str.contains('deposits'))]

def main():
    # Connect to MongoDB
    mongodb = connect_to_mongodb('mongodb', 27017, 'tpch')
    mongodb_customers = get_customers_from_mongodb(mongodb)

    # Connect to Redis
    redis_conn = connect_to_redis('redis', 6379, '0')
    redis_orders = get_orders_from_redis(redis_conn)

    # Merge data and query logic
    orders_count = redis_orders['O_CUSTKEY'].value_counts().reset_index()
    orders_count.columns = ['C_CUSTKEY', 'number_of_orders']

    # Aggregate to find number of customers per number of orders
    distribution = orders_count['number_of_orders'].value_counts().reset_index()
    distribution.columns = ['number_of_orders', 'number_of_customers']

    # Write output to CSV
    distribution.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
