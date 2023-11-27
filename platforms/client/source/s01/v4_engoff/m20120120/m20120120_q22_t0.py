from pymongo import MongoClient
import direct_redis
import pandas as pd
from datetime import datetime, timedelta

# MongoDB connection & query
client = MongoClient('mongodb', 27017)
db = client['tpch']

customer_data = list(db.customer.find(
    {"$and": [
        {"C_PHONE": {"$regex": "^(20|40|22|30|39|42|21)"}},
        {"C_ACCTBAL": {"$gt": 0.0}}
    ]},
    {"C_CUSTKEY": 1, "_id": 0, "C_PHONE": 1, "C_ACCTBAL": 1}
))
customer_df = pd.DataFrame(customer_data)
# Extract the country code
customer_df['country_code'] = customer_df['C_PHONE'].str[:2]

# Redis connection & query
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
orders_df = pd.read_json(r.get('orders').decode('utf-8'))
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter orders that are at least 7 years old and convert to DataFrame
seven_years_ago = datetime.now() - timedelta(days=7*365)
old_orders_df = orders_df[orders_df['O_ORDERDATE'] <= seven_years_ago]

# Merge dataframes and perform the aggregation
merged_df = customer_df.merge(old_orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result_df = merged_df.groupby('country_code').agg(
    customers_without_orders=('C_CUSTKEY', 'size'),
    avg_balance=('C_ACCTBAL', 'mean')
).reset_index()

# Save results to CSV
result_df.to_csv('query_output.csv', index=False)
