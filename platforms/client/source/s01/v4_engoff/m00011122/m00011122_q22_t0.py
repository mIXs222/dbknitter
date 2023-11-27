# global_sales_opportunity_query.py

import pandas as pd
import pymongo
from direct_redis import DirectRedis
from datetime import datetime, timedelta

# MongoDB connection and querying
def get_mongodb_customers(mongodb_host, mongodb_port, db_name):
    client = pymongo.MongoClient(mongodb_host, mongodb_port)
    db = client[db_name]
    customer_data = list(db.customer.find({}, {'_id': 0}))
    return pd.DataFrame(customer_data)

# Redis connection and querying
def get_redis_orders(redis_host, redis_port, db_name):
    redis_client = DirectRedis(host=redis_host, port=redis_port, db=db_name)
    orders_data = redis_client.get('orders')
    return pd.DataFrame(orders_data)

# Define the country code and date cutoff for the customers
country_codes = ['20', '40', '22', '30', '39', '42', '21']
date_cutoff = (datetime.now() - timedelta(days=7*365)).strftime('%Y-%m-%d')

# Get data from MongoDB and Redis
mongo_customers = get_mongodb_customers('mongodb', 27017, 'tpch')
redis_orders = get_redis_orders('redis', 6379, 0)

# Filter customers based on conditions
filtered_customers = mongo_customers[
    mongo_customers.C_PHONE.str[:2].isin(country_codes) & 
    (mongo_customers.C_ACCTBAL > 0)
]

# Calculate the date cutoff for orders and filter orders without recent purchases
redis_orders['O_ORDERDATE'] = pd.to_datetime(redis_orders['O_ORDERDATE'])
filtered_orders = redis_orders[redis_orders['O_ORDERDATE'] >= date_cutoff]

# Find customers with no recent orders
customers_with_no_recent_orders = filtered_customers[
    ~filtered_customers['C_CUSTKEY'].isin(filtered_orders['O_CUSTKEY'])
]

# Group by country code and compute count and average balance
result = customers_with_no_recent_orders.groupby(
    customers_with_no_recent_orders.C_PHONE.str[:2]
).agg({'C_CUSTKEY': 'count', 'C_ACCTBAL': 'mean'}).reset_index()

result.columns = ['Country_Code', 'Customer_Count', 'Average_Balance']
# Save to CSV
result.to_csv('query_output.csv', index=False)
