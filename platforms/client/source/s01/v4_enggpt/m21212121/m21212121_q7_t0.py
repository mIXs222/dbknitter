# query.py

import pandas as pd
from pymongo import MongoClient
from direct_redis import DirectRedis
import datetime


# Connect to MongoDB
def mongodb_connect(dbname, host, port):
    client = MongoClient(host=host, port=port)
    db = client[dbname]
    return db


# Connect to Redis
def redis_connect(dbname, host, port):
    redis = DirectRedis(host=host, port=port, db=dbname)
    return redis


# Get data from MongoDB databas
def get_mongo_data(db):
    supplier_data = pd.DataFrame(list(db.supplier.find({"S_NATIONKEY": {"$in": [5, 24]}})))
    lineitem_data = pd.DataFrame(list(db.lineitem.find({
        "L_SHIPDATE": {"$gte": datetime.datetime(1995, 1, 1),
                       "$lte": datetime.datetime(1996, 12, 31)}})))
    customer_data = pd.DataFrame(list(db.customer.find({"C_NATIONKEY": {"$in": [5, 24]}})))
    return supplier_data, lineitem_data, customer_data


# Get data from Redis database
def get_redis_data(redis):
    nation_data = pd.read_json(redis.get('nation'))
    orders_data = pd.read_json(redis.get('orders'))
    return nation_data, orders_data


# Analysis process
def process_data(supplier_data, lineitem_data, customer_data, nation_data, orders_data):
    # Filter and rename columns for join
    supplier_data = supplier_data.rename(columns={"S_SUPPKEY": "L_SUPPKEY", "S_NATIONKEY": "N_NATIONKEY_SUPPLIER"})
    customer_data = customer_data.rename(columns={"C_CUSTKEY": "O_CUSTKEY", "C_NATIONKEY": "N_NATIONKEY_CUSTOMER"})

    # Calculate revenue volume
    lineitem_data['REVENUE'] = lineitem_data['L_EXTENDEDPRICE'] * (1 - lineitem_data['L_DISCOUNT'])

    # Merge datasets
    merged_data = lineitem_data.merge(supplier_data, on='L_SUPPKEY', how='inner')
    merged_data = merged_data.merge(orders_data, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')
    merged_data = merged_data.merge(customer_data, on='O_CUSTKEY', how='inner')
    merged_data = merged_data.merge(nation_data, left_on='N_NATIONKEY_SUPPLIER', right_on='N_NATIONKEY', how='inner')
    merged_data = merged_data.merge(nation_data, left_on='N_NATIONKEY_CUSTOMER', right_on='N_NATIONKEY', how='inner', suffixes=('_SUPPLIER', '_CUSTOMER'))

    # Filter for the desired nations and dates
    filtered_data = merged_data[
        (merged_data['N_NAME_SUPPLIER'].isin(['JAPAN', 'INDIA'])) &
        (merged_data['N_NAME_CUSTOMER'].isin(['JAPAN', 'INDIA'])) &
        (merged_data['N_NAME_SUPPLIER'] != merged_data['N_NAME_CUSTOMER'])
    ]

    # Extract year from L_SHIPDATE and group by required fields
    filtered_data['YEAR'] = filtered_data['L_SHIPDATE'].dt.year
    final_data = filtered_data.groupby(['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'YEAR']).agg({'REVENUE': 'sum'}).reset_index()

    # Sort the results
    final_data.sort_values(by=['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'YEAR'], inplace=True)

    # Write output to a CSV file
    final_data.to_csv('query_output.csv', index=False)


def main():
    # MongoDB
    mongo_db = mongodb_connect(dbname='tpch', host='mongodb', port=27017)
    supplier_data_mongo, lineitem_data_mongo, customer_data_mongo = get_mongo_data(mongo_db)

    # Redis
    redis_db = redis_connect(dbname=0, host='redis', port=6379)
    nation_data_redis, orders_data_redis = get_redis_data(redis_db)

    # Process and output data
    process_data(supplier_data_mongo, lineitem_data_mongo, customer_data_mongo, nation_data_redis, orders_data_redis)


if __name__ == "__main__":
    main()
