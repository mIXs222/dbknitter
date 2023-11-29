import pymongo
import pandas as pd
from direct_redis import DirectRedis
import json
import csv

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongo_db = client['tpch']
customer_col = mongo_db['customer']
lineitem_col = mongo_db['lineitem']

# Fetch building market segment customer keys from MongoDB
building_customers = list(customer_col.find({"C_MKTSEGMENT": "BUILDING"}, {"_id": 0, "C_CUSTKEY": 1}))
building_customer_keys = [customer['C_CUSTKEY'] for customer in building_customers]

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Fetch orders information from Redis
orders_str = r.get('orders')
orders_data = json.loads(orders_str)
orders = pd.DataFrame(orders_data)

# Convert dates to pandas datetime format
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])

# Filter orders based on customer keys and date conditions
filtered_orders = orders[(orders['O_CUSTKEY'].isin(building_customer_keys)) &
                         (orders['O_ORDERDATE'] < '1995-03-05')]

# Fetch line items information from MongoDB
lineitem_data = list(lineitem_col.find({"L_SHIPDATE": {"$gt": "1995-03-15"}},
                                       {"_id": 0, "L_ORDERKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1}))
lineitem_df = pd.DataFrame(lineitem_data)

# Calculate Revenue
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Join orders with lineitem on order key
result_df = pd.merge(filtered_orders, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by order key and calculate total revenue
result_df = result_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'], as_index=False)['REVENUE'].sum()

# Sort by revenue in descending order
result_df = result_df.sort_values(by='REVENUE', ascending=False)

# Select and rename final columns
final_df = result_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

# Write the results to a CSV file
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
