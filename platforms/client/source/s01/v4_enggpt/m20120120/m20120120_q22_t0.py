# query.py

import pymongo
import pandas as pd
import redis
from io import StringIO

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client['tpch']
customer_coll = mongodb['customer']

# Connect to Redis
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# Fetch customer data from MongoDB
customers = pd.DataFrame(list(customer_coll.find()))

# Extract country codes from phone numbers and add as a new column
customers['CNTRYCODE'] = customers['C_PHONE'].str[:2]

# Calculate average account balance for specified country codes
codes = ['20', '40', '22', '30', '39', '42', '21']
avg_balances = customers[
    (customers['C_ACCTBAL'] > 0) & (customers['CNTRYCODE'].isin(codes))
].groupby('CNTRYCODE')['C_ACCTBAL'].mean().to_dict()

# Apply filter for customers with account balance greater than the average
customers = customers[
    customers.apply(lambda x: x['C_ACCTBAL'] > avg_balances.get(x['CNTRYCODE'], 0), axis=1)
]

# Fetch orders data from Redis and convert to DataFrame
orders = pd.read_json(StringIO(r.get('orders')), orient='records')

# Identify customers who have not placed any orders
customers_with_no_orders = customers[
    ~customers['C_CUSTKEY'].isin(orders['O_CUSTKEY'])
]

# Aggregate the results
result = customers_with_no_orders.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum'),
)

# Order the results
result = result.sort_values(by='CNTRYCODE').reset_index()

# Write the results to a CSV file
result.to_csv('query_output.csv', index=False)
