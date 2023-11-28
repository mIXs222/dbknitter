import pymongo
from pymongo import MongoClient
import redis
import pandas as pd
from io import StringIO

# Connection to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']

# Connection to Redis
r = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Fetch data from redis
try:
    customer_data = r.get('customer')
    customer_df = pd.read_json(StringIO(customer_data))
except Exception as e:
    print(f'Error retrieving customer data from Redis: {e}')
    customer_df = pd.DataFrame()

# Transform customer data to include country codes and filter by positive balances
customer_df['CNTRYCODE'] = customer_df['C_PHONE'].str[:2]
country_codes = ['20', '40', '22', '30', '39', '42', '21']
positive_balances_df = customer_df[customer_df['C_ACCTBAL'] > 0]
average_balances = positive_balances_df.groupby('CNTRYCODE')['C_ACCTBAL'].mean().reset_index()

# Get customers with balance greater than the average in the specified country codes
average_balances = average_balances[average_balances['CNTRYCODE'].isin(country_codes)]
customers_above_average = customer_df.merge(average_balances, on='CNTRYCODE', suffixes=('', '_AVG'))

final_customers = customers_above_average[
    (customers_above_average['C_ACCTBAL'] > customers_above_average['C_ACCTBAL_AVG']) &
    (customers_above_average['CNTRYCODE'].isin(country_codes))
][['C_CUSTKEY', 'C_ACCTBAL', 'CNTRYCODE']]

# Get orders by customer key
orders_df = pd.DataFrame(list(orders_collection.find({}, {'_id': 0, 'O_CUSTKEY': 1})))

# Filter out customers who have placed orders
final_customers = final_customers[~final_customers['C_CUSTKEY'].isin(orders_df['O_CUSTKEY'])]

# Summarize the results by country code
summary_df = final_customers.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index().sort_values(by='CNTRYCODE')

# Write the query's output to CSV file
summary_df.to_csv('query_output.csv', index=False)
