import pymongo
from direct_redis import DirectRedis
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongo_db = client["tpch"]
customer_collection = mongo_db["customer"]

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Get customer data from MongoDB
customer_df = pd.DataFrame(list(customer_collection.find({})), columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'])

# Get order data from Redis as DataFrame, assuming that data is stored in a way that pandas can understand
# Extracted data from Redis would need to be converted from string or bytes to a format that pandas can understand
orders_data = redis_connection.get('orders')

# Assuming that orders_data is already in a format that can be read directly by pandas
orders_df = pd.read_json(orders_data, orient='records')

# Merge DataFrames on customer key
merged_df = pd.merge(customer_df, orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Exclude pending and deposits orders
filtered_df = merged_df[~merged_df['O_COMMENT'].str.contains('pending|deposits', na=False)]

# Count orders per customer
order_counts = filtered_df.groupby('C_CUSTKEY')['O_ORDERKEY'].nunique().reset_index(name='order_count')

# Count the distribution of order counts, including customers with zero orders
distribution_counts = order_counts['order_count'].value_counts().sort_index().reset_index(name='customer_count')
distribution_counts.rename(columns={'index': 'number_of_orders'}, inplace=True)

# Write to CSV
distribution_counts.to_csv('query_output.csv', index=False)
