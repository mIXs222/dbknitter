from pymongo import MongoClient
from redis.commands.json.path import Path
import direct_redis
import pandas as pd
from datetime import datetime
import csv

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MongoDB
customers = list(mongo_db.customer.find())
orders = list(mongo_db.orders.find({
    "O_ORDERDATE": {
        "$gte": datetime(1993, 10, 1),
        "$lte": datetime(1993, 12, 31)
    }
}))
lineitems = list(mongo_db.lineitem.find({
    "L_RETURNFLAG": "R"
}))

# Convert lists to DataFrames
df_customers = pd.DataFrame(customers)
df_orders = pd.DataFrame(orders)
df_lineitems = pd.DataFrame(lineitems)

# Retrieve nation data from Redis
nations_json = redis_client.json().get('nation', Path.rootPath())
df_nations = pd.DataFrame(nations_json)

# Merge dataframes
merged_order_lineitem = pd.merge(df_orders, df_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_customer_order = pd.merge(df_customers, merged_order_lineitem, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
final_merge = pd.merge(merged_customer_order, df_nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Compute total revenue
final_merge['REVENUE'] = final_merge['L_EXTENDEDPRICE'] * (1 - final_merge['L_DISCOUNT'])

# Select and rename the required columns
analysis = final_merge[[
    'C_CUSTKEY',
    'C_NAME',
    'REVENUE',
    'C_ACCTBAL',
    'N_NAME',
    'C_ADDRESS',
    'C_PHONE',
    'C_COMMENT'
]].rename(columns={
    'C_CUSTKEY': 'Customer Key',
    'C_NAME': 'Customer Name',
    'REVENUE': 'Total Revenue',
    'C_ACCTBAL': 'Account Balance',
    'N_NAME': 'Nation Name',
    'C_ADDRESS': 'Address',
    'C_PHONE': 'Phone Number',
    'C_COMMENT': 'Comments'
})

# Group by customer key and calculate total revenue per customer
grouped_analysis = analysis.groupby([
    'Customer Key', 
    'Customer Name', 
    'Account Balance', 
    'Phone Number', 
    'Nation Name', 
    'Address', 
    'Comments'
]).agg({'Total Revenue': 'sum'}).reset_index()

# Sort results
sorted_analysis = grouped_analysis.sort_values(by=['Total Revenue', 'Customer Key', 'Customer Name', 'Account Balance'], ascending=[True, True, True, False])

# Write to CSV file
sorted_analysis.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
