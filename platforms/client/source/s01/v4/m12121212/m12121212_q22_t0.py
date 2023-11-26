import pymongo
import redis
import pandas as pd

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]
orders_data = list(orders_collection.find(
  {},
  {"_id": 0, "O_CUSTKEY": 1}
))
orders_df = pd.DataFrame(orders_data)

# Connect to Redis
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Get customer data from Redis and convert it to a Pandas DataFrame
customers = pd.read_json(redis_client.get('customer'))

# Filter customers based on the conditions from SQL Query
countries = ('20', '40', '22', '30', '39', '42', '21')
filtered_customers = customers[
    customers['C_PHONE'].str[:2].isin(countries) &
    (customers['C_ACCTBAL'] > 0.00)
]

# Compute average C_ACCTBAL where C_ACCTBAL > 0 for selected countries
avg_acct_bal = filtered_customers['C_ACCTBAL'].mean()

# Further filter customers based on average account balance
final_customers = filtered_customers[
    filtered_customers['C_ACCTBAL'] > avg_acct_bal
]

# Exclude customers that exist in orders
customers_without_orders = final_customers[
    ~final_customers['C_CUSTKEY'].isin(orders_df['O_CUSTKEY'])
]

# Add CNTRYCODE to customers_without_orders
customers_without_orders['CNTRYCODE'] = customers_without_orders['C_PHONE'].str[:2]

# Group by CNTRYCODE and perform aggregations
result = customers_without_orders.groupby('CNTRYCODE').agg(
    NUMCUST=('CNTRYCODE', 'size'),
    TOTACCTBAL=('C_ACCTBAL', 'sum')
).reset_index()

# Sort by CNTRYCODE
result = result.sort_values('CNTRYCODE')

# Write the output to a CSV file
result.to_csv('query_output.csv', index=False)
