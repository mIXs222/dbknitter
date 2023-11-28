import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from MongoDB
customers = pd.DataFrame(list(mongo_db.customer.find({}, {'_id': 0})))
orders = pd.DataFrame(list(mongo_db.orders.find({}, {'_id': 0})))
lineitem = pd.DataFrame(list(mongo_db.lineitem.find({}, {'_id': 0})))

# Get data from Redis and convert to DataFrame
nation = pd.read_json(redis_client.get('nation'))
supplier = pd.read_json(redis_client.get('supplier'))

# Filter data for the specified years
start_date = datetime(1995, 1, 1)
end_date = datetime(1996, 12, 31)
lineitem = lineitem[((lineitem['L_SHIPDATE'] >= start_date) & (lineitem['L_SHIPDATE'] <= end_date))]

# Compute revenue
lineitem['REVENUE'] = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])

# Merge dataframes
merged_df = lineitem.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY').merge(customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY').merge(supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY').merge(nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY').merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_CUSTOMER', '_SUPPLIER'))

# Filter for nations 'JAPAN' and 'INDIA' in supplier/customer capacities
filtered_df = merged_df[
    ((merged_df['N_NAME_SUPPLIER'] == 'JAPAN') & (merged_df['N_NAME_CUSTOMER'] == 'INDIA')) |
    ((merged_df['N_NAME_SUPPLIER'] == 'INDIA') & (merged_df['N_NAME_CUSTOMER'] == 'JAPAN'))
]

# Group by the required fields
grouped_df = filtered_df.groupby([
    'N_NAME_SUPPLIER', 
    'N_NAME_CUSTOMER', 
    pd.Grouper(key='L_SHIPDATE', freq='Y')
]).agg({'REVENUE': 'sum'}).reset_index()

# Rename columns to align with SQL's YEAR() function
grouped_df['L_SHIPDATE'] = grouped_df['L_SHIPDATE'].dt.year
grouped_df.rename(columns={'L_SHIPDATE': 'YEAR_OF_SHIPPING'}, inplace=True)

# Sort the results as required
sorted_df = grouped_df.sort_values(by=['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'YEAR_OF_SHIPPING'])

# Write output to CSV
sorted_df.to_csv('query_output.csv', index=False)
