import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client['tpch']
orders_collection = mongo_db['orders']

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Load data from MongoDB
orders_query = {
    "O_ORDERDATE": {"$lt": datetime.now() - pd.DateOffset(years=7)}
}
orders_df = pd.DataFrame(list(orders_collection.find(orders_query, {'_id': 0, 'O_CUSTKEY': 1})))

# Load data from Redis (assuming 'customer' is stored as a Pandas DataFrame in Redis)
customer_df_bytes = r.get('customer')
customer_df = pd.read_msgpack(customer_df_bytes)

# Filter customers based on country codes and with account balance greater than 0.00
country_codes = ['20', '40', '22', '30', '39', '42', '21']
customer_df['C_COUNTRYCODE'] = customer_df['C_PHONE'].str[:2]
filtered_customers_df = customer_df[
    (customer_df['C_COUNTRYCODE'].isin(country_codes)) &
    (customer_df['C_ACCTBAL'] > 0.00)
]

# Merge to find customers without orders
merged_df = pd.merge(
    filtered_customers_df,
    orders_df,
    how="left",
    left_on='C_CUSTKEY',
    right_on='O_CUSTKEY'
)

# Customers who have not placed orders
result_df = merged_df[merged_df['O_ORDERKEY'].isna()]

# Group by country code and compute count and average balance
output_df = result_df.groupby('C_COUNTRYCODE').agg({
    'C_CUSTKEY': 'count',
    'C_ACCTBAL': 'mean'
}).reset_index()

# Rename columns to reflect the content more appropriately
output_df.columns = ['CountryCode', 'CustomerCount', 'AverageAccountBalance']

# Write output to CSV
output_df.to_csv('query_output.csv', index=False)
