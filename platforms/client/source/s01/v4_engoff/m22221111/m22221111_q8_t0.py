# market_share.py

from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# MongoDB connection and query
def get_mongo_data():
    client = MongoClient(host="mongodb", port=27017)
    db = client.tpch

    # Filters for products of "SMALL PLATED COPPER" and "INDA"
    lineitem_filter = {'L_SHIPDATE': {'$in': ['1995-12-31', '1996-12-31']}}
    orders_filter = {'O_ORDERDATE': {'$regex': '.*199[56].*'}}  # Orders in 1995 and 1996
    supplier_filter = {'S_NAME': {'$regex': '.*INDA.*'}}

    # Projection for the necessary fields
    lineitem_projection = {'_id': False, 'L_ORDERKEY': True, 'L_EXTENDEDPRICE': True, 'L_DISCOUNT': True}
    orders_projection = {'_id': False, 'O_ORDERKEY': True}

    # Fetch the required data from collections
    lineitems = list(db.lineitem.find(lineitem_filter, lineitem_projection))
    orders = list(db.orders.find(orders_filter, orders_projection))

    # Creating DataFrames
    df_lineitems = pd.DataFrame(lineitems)
    df_orders = pd.DataFrame(orders)

    # Merge DataFrames on the order key
    lineitem_orders_df = pd.merge(df_lineitems, df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')

    return lineitem_orders_df

# Redis connection and query
def get_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)

    # Fetching data from Redis, assuming they are stored in appropriate keys
    df_nation = pd.DataFrame(eval(redis_client.get('nation')))
    df_region = pd.DataFrame(eval(redis_client.get('region')))
    df_supplier = pd.DataFrame(eval(redis_client.get('supplier')))

    # Filter and join operations (we would need to reformat the Redis data to fit the SQL-like query if they are not already in a DataFrame format)
    asia_region = df_region[df_region['R_NAME'] == 'ASIA']
    india_nations = df_nation[df_nation['N_NAME'] == 'INDIA']

    # Merge to get suppliers from INDIA in ASIA
    suppliers_asia_india = pd.merge(df_supplier, india_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    suppliers_asia_india = pd.merge(suppliers_asia_india, asia_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

    return suppliers_asia_india[['S_SUPPKEY']]

# Combine and compute market share
def compute_market_share(lineitem_orders_df, suppliers_asia_india):
    # Combine MongoDB and Redis data
    combined_df = pd.merge(lineitem_orders_df, suppliers_asia_india, left_on='L_SUPPKEY', right_on='S_SUPPKEY', how='inner')

    # Compute the revenue
    combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])

    # Calculate the market share for each year
    combined_df['YEAR'] = combined_df['O_ORDERDATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').year)
    
    market_share = combined_df.groupby('YEAR')['REVENUE'].sum() / combined_df['REVENUE'].sum()

    return market_share

def main():
    lineitem_orders_df = get_mongo_data()
    suppliers_asia_india = get_redis_data()
    market_share = compute_market_share(lineitem_orders_df, suppliers_asia_india)

    # Save to query_output.csv
    market_share.to_csv('query_output.csv', header=True)

if __name__ == '__main__':
    main()
