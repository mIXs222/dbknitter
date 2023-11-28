# Python code to execute the complex multi-database query
import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Function to get the dataframe from Redis
def get_redis_df(redis_client, table_name):
    data = redis_client.get(table_name)
    if data:
        return pd.read_json(data)
    else:
        return pd.DataFrame()

# Connecting to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]
supplier_df = pd.DataFrame(list(mongodb.supplier.find({})))
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find({})))

# Connecting to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = get_redis_df(redis_client, 'nation')
orders_df = get_redis_df(redis_client, 'orders')

# Process the query
# Filter the nations for 'SAUDI ARABIA'
saudi_suppliers = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']['N_NATIONKEY'].unique()

# Filter suppliers based on location
suppliers_in_saudi = supplier_df[supplier_df['S_NATIONKEY'].isin(saudi_suppliers)]

# Join suppliers with lineitem
suppliers_lineitems = suppliers_in_saudi.merge(lineitem_df, how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Join the result with orders
suppliers_orders = suppliers_lineitems.merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Final filtering according to the specified conditions
result_df = suppliers_orders[
    (suppliers_orders['O_ORDERSTATUS'] == 'F') &
    (suppliers_orders['L_RECEIPTDATE'] > suppliers_orders['L_COMMITDATE'])
].groupby('S_NAME').agg(NUMWAIT=('L_ORDERKEY', 'count')).reset_index()

# Result ordering
result_df = result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Writing to CSV
result_df.to_csv('query_output.csv', index=False)
