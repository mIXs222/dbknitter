import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']
customer_collection = mongodb['customer']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get customer data from MongoDB
customer_df = pd.DataFrame(list(customer_collection.find({}, {
    '_id': 0,
    'C_CUSTKEY': 1,
    'C_NAME': 1,
    'C_ADDRESS': 1,
    'C_NATIONKEY': 1,
    'C_PHONE': 1,
    'C_ACCTBAL': 1,
    'C_MKTSEGMENT': 1,
    'C_COMMENT': 1
})))

# Get orders data from Redis
orders_df = pd.read_msgpack(redis_client.get('orders'))

# Filter out orders with comments containing 'pending' or 'deposits'
filtered_orders_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', case=False, na=False)]

# Perform left join on the customer and orders dataframes
merged_df = pd.merge(customer_df, filtered_orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Group by customer key and count the number of orders
order_counts = merged_df.groupby('C_CUSTKEY').agg({'O_ORDERKEY': pd.Series.nunique}).reset_index()
order_counts.rename(columns={'O_ORDERKEY': 'C_COUNT'}, inplace=True)

# Add customer information back to the order counts
customer_order_counts = pd.merge(customer_df[['C_CUSTKEY']], order_counts, on='C_CUSTKEY', how='left')

# Replace NaN with 0 in C_COUNT
customer_order_counts['C_COUNT'] = customer_order_counts['C_COUNT'].fillna(0)

# Calculate the distribution of customers by order count
distribution = customer_order_counts.groupby('C_COUNT').size().reset_index(name='CUSTDIST')

# Sort the distribution in descending order by CUSTDIST and C_COUNT
sorted_distribution = distribution.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Save the results to query_output.csv
sorted_distribution.to_csv('query_output.csv', index=False)
