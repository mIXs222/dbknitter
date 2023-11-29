from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
import datetime

# MongoDB Connection
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Fetch nation data from MongoDB
nation_data = db.nation.find({}, {'_id': 0})
nation_df = pd.DataFrame(list(nation_data))

# Fetch orders data from MongoDB
orders_data = db.orders.find({}, {'_id': 0})
orders_df = pd.DataFrame(list(orders_data))

# Initialize DirectRedis for Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch supplier data from Redis
supplier_df = pd.read_json(redis_client.get('supplier'))

# Fetch customer data from Redis
customer_df = pd.read_json(redis_client.get('customer'))

# Fetch lineitem data from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Preprocess and convert dates to datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter nations for 'INDIA' and 'JAPAN'
filtered_nation_df = nation_df[nation_df['N_NAME'].isin(['INDIA', 'JAPAN'])]

# Combine datasets to perform the query
lineitem_supplier_df = lineitem_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
lineitem_supplier_customer_df = lineitem_supplier_df.merge(customer_df, left_on='L_ORDERKEY', right_on='C_CUSTKEY')
lineitem_supplier_customer_order_df = lineitem_supplier_customer_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
final_df = lineitem_supplier_customer_order_df.merge(filtered_nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter the date for the years 1995 and 1996
final_df = final_df[(final_df['O_ORDERDATE'].dt.year == 1995) | (final_df['O_ORDERDATE'].dt.year == 1996)]

# Calculate revenue
final_df['REVENUE'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])

# Filter cross shippings between INDIA and JAPAN
condition = ((final_df['N_NAME_x'] == 'INDIA') & (final_df['N_NAME_y'] == 'JAPAN')) | \
            ((final_df['N_NAME_x'] == 'JAPAN') & (final_df['N_NAME_y'] == 'INDIA'))

final_result = final_df[condition]

# Select required columns and rename
result = final_result[['N_NAME_y', 'O_ORDERDATE', 'REVENUE', 'N_NAME_x']]
result.rename(columns={'N_NAME_y':'CUST_NATION', 'O_ORDERDATE':'L_YEAR', 'N_NAME_x':'SUPP_NATION'}, inplace=True)
result['L_YEAR'] = result['L_YEAR'].dt.year

# Sort the result
result = result.sort_values(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write the result to a CSV file
result.to_csv('query_output.csv', index=False)
