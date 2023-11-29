from pymongo import MongoClient
import pandas as pd
import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']
nation_collection = mongodb['nation']

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Load data from MongoDB
nation_data = pd.DataFrame(list(nation_collection.find({}, {'_id': 0})))

# Load data from Redis into Pandas DataFrames
customer_df = pd.DataFrame(eval(r.get('customer')))
orders_df = pd.DataFrame(eval(r.get('orders')))
lineitem_df = pd.DataFrame(eval(r.get('lineitem')))

# Convert dates to datetime objects in orders and lineitem dataframes
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter orders made in the specific quarter and lineitems returned
orders_filtered = orders_df[
    (orders_df['O_ORDERDATE'] >= datetime.datetime(1993, 10, 1)) &
    (orders_df['O_ORDERDATE'] <= datetime.datetime(1994, 1, 1))
]
returned_lineitems = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']

# Calculate revenue lost
returned_lineitems['REVENUE_LOST'] = returned_lineitems['L_EXTENDEDPRICE'] * (1 - returned_lineitems['L_DISCOUNT'])

# Combine datasets
joined_df = (returned_lineitems
             .merge(orders_filtered, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
             .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
             .merge(nation_data, left_on='C_NATIONKEY', right_on='N_NATIONKEY'))

# Select required columns for final output
final_df = joined_df[
    ['C_CUSTKEY', 'C_NAME', 'REVENUE_LOST', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']
]

# Calculate sum of revenue lost by customer
final_df = final_df.groupby(
    ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']
).agg({'REVENUE_LOST': 'sum'}).reset_index()

# Sort the final Dataframe as per query requirements
final_df.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
                     ascending=[True, True, True, False], inplace=True)

# Write the results to CSV
final_df.to_csv('query_output.csv', index=False)
