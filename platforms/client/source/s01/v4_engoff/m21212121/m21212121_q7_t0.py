import pymongo
import pandas as pd
import direct_redis

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongo_db = client['tpch']
supplier = pd.DataFrame(list(mongo_db.supplier.find()))
customer = pd.DataFrame(list(mongo_db.customer.find()))
lineitem = pd.DataFrame(list(mongo_db.lineitem.find()))

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation = pd.read_json(r.get('nation'))
orders = pd.read_json(r.get('orders'))

# Filtering for India and Japan nations
india_japan_nations = nation[(nation['N_NAME'] == 'INDIA') | (nation['N_NAME'] == 'JAPAN')]

# Merging customers with nations to get their nation names
customer_nation = customer.merge(india_japan_nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Merging suppliers with nations to get their nation names
supplier_nation = supplier.merge(india_japan_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filtering lineitems for date range and mering with orders
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])
lineitem_filtered = lineitem[(lineitem['L_SHIPDATE'] >= pd.Timestamp(year=1995, month=1, day=1)) & 
                             (lineitem['L_SHIPDATE'] <= pd.Timestamp(year=1996, month=12, day=31))]
lineitem_orders = lineitem_filtered.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Final merge to combine everything
result = lineitem_orders.merge(supplier_nation, left_on='L_SUPPKEY', right_on='S_SUPPKEY') \
                        .merge(customer_nation, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Calculate revenue and filter for shipments between India and Japan
result['revenue'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])
final_result = result[(result['N_NAME_x'] != result['N_NAME_y']) & 
                      ((result['N_NAME_x'].isin(['INDIA', 'JAPAN'])) & (result['N_NAME_y'].isin(['INDIA', 'JAPAN'])))]
final_result = final_result[['N_NAME_x', 'N_NAME_y', lineitem_orders['L_SHIPDATE'].dt.year, 'revenue']]

# Group by the necessary columns and sum up the revenues
grouped = final_result.groupby(['N_NAME_x', 'N_NAME_y', lineitem_orders['L_SHIPDATE'].dt.year])['revenue'].sum().reset_index()
grouped.columns = ['Supplier_Nation', 'Customer_Nation', 'Year', 'Revenue']

# Sort by Supplier Nation, Customer Nation, and Year
grouped_sorted = grouped.sort_values(by=['Supplier_Nation', 'Customer_Nation', 'Year'])

# Output the result to a CSV file
grouped_sorted.to_csv('query_output.csv', index=False)
