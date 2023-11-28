# Python code to execute the query

import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Function to connect to MongoDB
def get_mongodb_data():
    client = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = client["tpch"]
    lineitem_collection = db["lineitem"]
    lineitem_data = lineitem_collection.find({
        "L_SHIPMODE": {"$in": ["MAIL", "SHIP"]},
        "L_COMMITDATE": {"$lt": "$L_RECEIPTDATE"},
        "L_SHIPDATE": {"$lt": "$L_COMMITDATE"},
        "L_RECEIPTDATE": {"$gte": datetime(1994, 1, 1), "$lte": datetime(1994, 12, 31)}
    })
    return pd.DataFrame(list(lineitem_data))

# Function to connect to Redis and get data
def get_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    orders_data = redis_client.get('orders')
    return pd.read_json(orders_data)

# Combine data and perform analysis
def perform_analysis():
    # Get data from MongoDB and Redis
    lineitem_df = get_mongodb_data()
    orders_df = get_redis_data()

    # Merge lineitem and orders dataframe on order key
    merged_df = pd.merge(lineitem_df, orders_df, left_on="L_ORDERKEY", right_on="O_ORDERKEY")

    # Filtering for specific shipping modes
    filtered_df = merged_df[(merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP']))]

    # Grouping by shipping mode and priority
    grouped = filtered_df.groupby(['L_SHIPMODE', 'O_ORDERPRIORITY'])

    # Count high and low priority line items
    output_df = grouped['L_ORDERKEY'].agg(
        HIGH_LINE_COUNT=pd.NamedAgg(column='L_ORDERKEY', aggfunc=lambda x: x[filtered_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])].count()),
        LOW_LINE_COUNT=pd.NamedAgg(column='L_ORDERKEY', aggfunc=lambda x: x[~filtered_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])].count())
    ).reset_index()

    # Sorting results and writing to CSV
    output_df.sort_values('L_SHIPMODE', ascending=True).to_csv('query_output.csv', index=False)

perform_analysis()
