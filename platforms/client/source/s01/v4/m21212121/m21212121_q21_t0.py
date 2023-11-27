# query.py

import pymongo
import pandas as pd
import direct_redis
from datetime import datetime


def fetch_mongodb_data(mongo_client):
    db = mongo_client['tpch']
    supplier_df = pd.DataFrame(list(db.supplier.find(
        {}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_NATIONKEY': 1})))
    lineitem_df = pd.DataFrame(list(db.lineitem.find(
        {}, {'_id': 0, 'L_ORDERKEY': 1, 'L_SUPPKEY': 1, 'L_RECEIPTDATE': 1, 'L_COMMITDATE': 1})))

    return supplier_df, lineitem_df


def fetch_redis_data(redis_client):
    nation_df = pd.read_msgpack(redis_client.get('nation'))
    orders_df = pd.read_msgpack(redis_client.get('orders'))
    return nation_df, orders_df


def perform_query(supplier_df, lineitem_df, orders_df, nation_df):
    # Merge dataframes to simulate tables join
    merged_df = supplier_df.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    merged_df = merged_df.merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    merged_df = merged_df.merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    
    # Query conditions
    filtered_df = merged_df.query(
        "O_ORDERSTATUS == 'F' and L_RECEIPTDATE > L_COMMITDATE and N_NAME == 'SAUDI ARABIA'")

    # Subquery simulation
    subquery = lineitem_df[lineitem_df['L_SUPPKEY'] != lineitem_df['L_SUPPKEY']].drop_duplicates('L_ORDERKEY')
    filtered_df = filtered_df[filtered_df['L_ORDERKEY'].isin(subquery['L_ORDERKEY'])]

    not_subquery = lineitem_df[(lineitem_df['L_SUPPKEY'] != lineitem_df['L_SUPPKEY']) &
                               (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE'])]
    filtered_df = filtered_df[~filtered_df['L_ORDERKEY'].isin(not_subquery['L_ORDERKEY'])]

    # Group by and count
    result_df = filtered_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')

    # Sorting the results
    result_df = result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

    return result_df


def main():
    # Connect to MongoDB
    mongo_client = pymongo.MongoClient('mongodb', 27017)

    # Connect to Redis
    redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Fetch data from databases
    supplier_df, lineitem_df = fetch_mongodb_data(mongo_client)
    nation_df, orders_df = fetch_redis_data(redis_client)

    # Perform the query
    result_df = perform_query(supplier_df, lineitem_df, orders_df, nation_df)

    # Write result to CSV
    result_df.to_csv('query_output.csv', index=False)


if __name__ == "__main__":
    main()
