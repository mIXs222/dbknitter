import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
collection_nation = mongodb["nation"]
collection_supplier = mongodb["supplier"]

# Connection to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MongoDB
nation_df = pd.DataFrame(list(collection_nation.find({}, {'_id': 0})))
supplier_df = pd.DataFrame(list(collection_supplier.find({}, {'_id': 0})))

# Filter nations for 'INDIA' and 'JAPAN'
nations_of_interest = nation_df[nation_df['N_NAME'].isin(['INDIA', 'JAPAN'])]

# Retrieve data from Redis
customer_df = pd.read_json(r.get('customer'))
orders_df = pd.read_json(r.get('orders'))
lineitem_df = pd.read_json(r.get('lineitem'))

customer_df = customer_df.merge(nations_of_interest, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')
supplier_df = supplier_df.merge(nations_of_interest, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')

# Join the tables
result_df = orders_df.merge(customer_df[['C_CUSTKEY', 'C_NAME', 'N_NAME']], on='C_CUSTKEY')
result_df = result_df.merge(lineitem_df, on='O_ORDERKEY')
result_df = result_df.merge(supplier_df[['S_SUPPKEY', 'S_NAME', 'N_NAME']], left_on='L_SUPPKEY', right_on='S_SUPPKEY')
result_df['L_YEAR'] = pd.to_datetime(result_df['L_SHIPDATE']).dt.year
result_df = result_df[(result_df['L_YEAR'] == 1995) | (result_df['L_YEAR'] == 1996)]

# Calculate revenue and filter as per the condition
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Re-arrange columns and rename as per requirement
result_df = result_df.rename(columns={'N_NAME_x': 'CUST_NATION', 'N_NAME_y': 'SUPP_NATION'})
result_df = result_df[['CUST_NATION', 'SUPP_NATION', 'L_YEAR', 'REVENUE']]

# Filter such that supplier and customer nations are different and either 'INDIA' or 'JAPAN'
result_df = result_df[(result_df['CUST_NATION'] != result_df['SUPP_NATION']) & 
                      ((result_df['CUST_NATION'].isin(['INDIA', 'JAPAN'])) | (result_df['SUPP_NATION'].isin(['INDIA', 'JAPAN'])))]

# Group by and sum
result_df = result_df.groupby(['CUST_NATION', 'SUPP_NATION', 'L_YEAR']).sum().reset_index()

# Sort as required
result_df.sort_values(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Write output to CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
