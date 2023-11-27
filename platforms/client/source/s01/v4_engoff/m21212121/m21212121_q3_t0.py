# shipping_priority_query.py

import pymongo
from pymongo import MongoClient
import direct_redis
import pandas as pd
from datetime import datetime

def get_mongodb_data(client, table_name):
    db = client['tpch']
    return pd.DataFrame(list(db[table_name].find()))

def get_redis_data(redis_client, table_name):
    data = redis_client.get(table_name)
    return pd.read_json(data, orient='records')

def calculate_revenue(lineitem):
    lineitem['REVENUE'] = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    return lineitem

def shipping_priority_query(mongo_client, redis_client):
    # Get data from MongoDB
    customers = get_mongodb_data(mongo_client, 'customer')
    lineitem = get_mongodb_data(mongo_client, 'lineitem')

    # Get data from Redis
    orders = get_redis_data(redis_client, 'orders')

    # Calculate revenue for lineitem
    lineitem = calculate_revenue(lineitem)
    
    # Convert type for join operations
    orders['O_ORDERKEY'] = orders['O_ORDERKEY'].astype(str)
    lineitem['L_ORDERKEY'] = lineitem['L_ORDERKEY'].astype(str)

    # Join data on 'orderkey' and filter by given conditions
    filters = (orders['O_ORDERDATE'] <= datetime(1995, 3, 15)) & \
              (customers['C_MKTSEGMENT'] == 'BUILDING') & \
              (lineitem['L_SHIPDATE'] > datetime(1995, 3, 15))
              
    combined_data = orders.merge(customers, how='left', left_on='O_CUSTKEY', right_on='C_CUSTKEY') \
        .merge(lineitem, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY') \
        .loc[filters, ['O_ORDERKEY', 'O_SHIPPRIORITY', 'REVENUE']]

    # Calculate the revenue among the non-shipped orders and sort in descending order of revenue
    result = combined_data.groupby(['O_ORDERKEY', 'O_SHIPPRIORITY']).sum().reset_index() \
        .sort_values(by='REVENUE', ascending=False)

    # Write to file
    result.to_csv('query_output.csv', index=False)

def main():
    # Connect to MongoDB
    mongo_client = MongoClient('mongodb', 27017)

    # Connect to Redis
    redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Run query
    shipping_priority_query(mongo_client, redis_client)

if __name__ == "__main__":
    main()
