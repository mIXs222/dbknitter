import pandas as pd
import pymongo
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_nation_coll = mongo_db['nation']

# Get the nation data
nation_data = pd.DataFrame(list(mongo_nation_coll.find({}, {'_id': 0})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Deserialize DataFrame stored in Redis format
def get_df_from_redis(key):
    data = redis_client.get(key)
    if data:
        return pd.read_msgpack(data)
    return None

# Get the customer, orders, and lineitem data from Redis
customer_df = get_df_from_redis('customer')
order_df = get_df_from_redis('orders')
lineitem_df = get_df_from_redis('lineitem')

# Filter orders and lineitems based on the given date range and order status
start_date = datetime.strptime('1993-10-01', '%Y-%m-%d')
end_date = datetime.strptime('1994-01-01', '%Y-%m-%d')

filtered_orders = order_df[(order_df['O_ORDERDATE'] >= start_date) & (order_df['O_ORDERDATE'] <= end_date)]
filtered_lineitems = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']  # Assuming 'R' stands for returned items

# Merge dataframes to calculate lost revenue
merged_df = pd.merge(filtered_orders, filtered_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df['LOST_REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Aggregate by customer to calculate total lost revenue per customer
agg_columns = {
    'LOST_REVENUE': 'sum',
    'C_CUSTKEY': 'first',
    'C_NAME': 'first',
    'C_ADDRESS': 'first',
    'C_PHONE': 'first',
    'C_ACCTBAL': 'first',
    'C_COMMENT': 'first',
}
final_df = merged_df.groupby('C_CUSTKEY', as_index=False).agg(agg_columns)
final_df = final_df.rename(columns={'C_CUSTKEY': 'CUSTOMER_KEY'})

# Merge with nation data to get nation name
result = pd.merge(final_df, nation_data, left_on='CUSTOMER_KEY', right_on='N_NATIONKEY')
result = result[['CUSTOMER_KEY', 'LOST_REVENUE', 'C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT']]

# Sort the result as specified
result = result.sort_values(by=['LOST_REVENUE', 'CUSTOMER_KEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True])

# Write the result to a CSV file
result.to_csv('query_output.csv', index=False)
