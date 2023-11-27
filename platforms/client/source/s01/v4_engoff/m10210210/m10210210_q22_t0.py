# execute_query.py
import pymongo
import pandas as pd
from datetime import datetime
import csv

# Connect to MongoDB
client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = client['tpch']
orders_collection = mongodb['orders']

# Query MongoDB for orders older than 7 years
seven_years_ago = datetime.now().year - 7
orders_query = {
    'O_ORDERDATE': {'$lt': datetime(seven_years_ago, 1, 1)}
}
orders_data = list(orders_collection.find(orders_query, {'_id': 0, 'O_CUSTKEY': 1}))
orders_df = pd.DataFrame(orders_data)

# Connect to Redis
import direct_redis
redis = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get customer data from Redis and load it into a dataframe
customer_data = redis.get('customer')
customer_df = pd.read_json(customer_data)

# Filter customers based on country codes and account balance
country_codes = ['20', '40', '22', '30', '39', '42', '21']
customer_df = customer_df[(customer_df['C_PHONE'].str[:2].isin(country_codes)) & (customer_df['C_ACCTBAL'] > 0)]

# Find customers who have not placed orders for 7 years
result_df = customer_df[~customer_df['C_CUSTKEY'].isin(orders_df['O_CUSTKEY'])]
# Aggregate result to get count and average balance
summary_df = result_df.groupby(customer_df['C_PHONE'].str[:2]) \
    .agg(Count=('C_CUSTKEY', 'size'), Average_Balance=('C_ACCTBAL', 'mean')) \
    .reset_index()

# Write results to CSV
summary_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
