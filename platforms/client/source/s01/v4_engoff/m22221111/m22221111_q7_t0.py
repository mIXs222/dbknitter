import pymongo
import csv
from datetime import datetime

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongodb = mongo_client['tpch']

# Redis connection with DirectRedis for pandas dataframe
from direct_redis import DirectRedis
redis_client = DirectRedis(host='redis', port=6379)

# Retrieve data from MongoDB
customers = list(mongodb['customer'].find())
orders = list(mongodb['orders'].find())
lineitems = list(mongodb['lineitem'].find())

# Retrieve data from Redis
import pandas as pd

nation_df = pd.read_msgpack(redis_client.get('nation'))
supplier_df = pd.read_msgpack(redis_client.get('supplier'))

# Convert to dataframe
customer_df = pd.DataFrame(customers)
order_df = pd.DataFrame(orders)
lineitem_df = pd.DataFrame(lineitems)

# Define nations of interest
nation_keys = {n['N_NATIONKEY']: n['N_NAME'] for _, n in nation_df.iterrows() if n['N_NAME'] in ['INDIA', 'JAPAN']}

# Join operations
supplier_nations = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_keys)]
customer_nations = customer_df[customer_df['C_NATIONKEY'].isin(nation_keys)]

ol_merged = order_df.merge(lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
oc_merged = ol_merged.merge(customer_nations, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
final_merge = oc_merged.merge(supplier_nations, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filter for year 1995 and 1996 and nations INDIA and JAPAN
final_merge['year'] = final_merge['L_SHIPDATE'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").year)
final_merge = final_merge[(final_merge['year'].isin([1995, 1996]))]
final_merge = final_merge[((final_merge['S_NATIONKEY'] != final_merge['C_NATIONKEY']) &
                          (final_merge['S_NATIONKEY'].isin(nation_keys)) &
                          (final_merge['C_NATIONKEY'].isin(nation_keys)))]

# Calculate revenue
final_merge['revenue'] = final_merge['L_EXTENDEDPRICE'] * (1 - final_merge['L_DISCOUNT'])

# Grouping and sorting
result = final_merge.groupby(['S_NATIONKEY', 'C_NATIONKEY', 'year'])['revenue'].sum().reset_index()
result['supplier_nation'] = result['S_NATIONKEY'].map(nation_keys)
result['customer_nation'] = result['C_NATIONKEY'].map(nation_keys)
result = result.sort_values(by=['supplier_nation', 'customer_nation', 'year'])

# Writing results to CSV
result[['supplier_nation', 'customer_nation', 'year', 'revenue']].to_csv('query_output.csv', index=False)
