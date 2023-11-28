import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connecting to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']
nation_col = db['nation']
supplier_col = db['supplier']

# Query MongoDB for nations 'JAPAN' and 'INDIA'
nations_df = pd.DataFrame(list(nation_col.find({'N_NAME': {'$in': ['JAPAN', 'INDIA']}})))

# Query MongoDB for suppliers from 'JAPAN' and 'INDIA'
suppliers_df = pd.DataFrame(list(supplier_col.find({'S_NATIONKEY': {'$in': nations_df['N_NATIONKEY'].tolist()}})))

# Connecting to Redis
r = DirectRedis(host='redis', port=6379)

# Get 'customer' and 'orders' tables from Redis
customer_df = pd.read_json(r.get('customer'))
orders_df = pd.read_json(r.get('orders'))

# Filtering the 'orders' DataFrame for the timeframe of interest
start_date = datetime(1995, 1, 1)
end_date = datetime(1996, 12, 31)
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_df = orders_df[(orders_df['O_ORDERDATE'] >= start_date) & (orders_df['O_ORDERDATE'] <= end_date)]

# Get 'lineitem' table from Redis
lineitem_df = pd.read_json(r.get('lineitem'))
# Calculating the revenue for each line item
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Merge data to get supplier and customer nation names
merged_df = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nations_df.add_prefix('SUPP_'), left_on='S_NATIONKEY', right_on='SUPP_N_NATIONKEY')
merged_df = merged_df.merge(nations_df.add_prefix('CUST_'), left_on='C_NATIONKEY', right_on='CUST_N_NATIONKEY')

# Filtering the DataFrame for 'JAPAN' and 'INDIA' nation pairs
nation_pairs = merged_df[((merged_df['SUPP_N_NAME'] == 'JAPAN') & (merged_df['CUST_N_NAME'] == 'INDIA')) |
                         ((merged_df['SUPP_N_NAME'] == 'INDIA') & (merged_df['CUST_N_NAME'] == 'JAPAN'))]

# Select desired columns, group by, and sort
final_df = nation_pairs[['SUPP_N_NAME', 'CUST_N_NAME', 'O_ORDERDATE', 'REVENUE']]
final_df['YEAR'] = final_df['O_ORDERDATE'].dt.year
grouped_df = final_df.groupby(['SUPP_N_NAME', 'CUST_N_NAME', 'YEAR']).agg({'REVENUE': 'sum'}).reset_index()

# Sorting as required
grouped_df.sort_values(by=['SUPP_N_NAME', 'CUST_N_NAME', 'YEAR'], inplace=True)

# Write the results to CSV
grouped_df.to_csv('query_output.csv', index=False)
