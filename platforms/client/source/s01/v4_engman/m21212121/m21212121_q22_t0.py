import pandas as pd
import pymongo
from datetime import datetime, timedelta
import direct_redis

# MongoDB connection and data retrieval
def get_mongodb_customers():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client['tpch']
    customer_df = pd.DataFrame(list(db.customer.find(
        {},
        {'_id': 0, 'C_CUSTKEY': 1, 'C_PHONE': 1, 'C_ACCTBAL': 1}
    )))
    client.close()
    return customer_df

# Redis connection and data retrieval
def get_redis_orders():
    dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    orders_df = dr.get('orders')
    return orders_df

# Process MongoDB data
customers_df = get_mongodb_customers()
customers_df['CNTRYCODE'] = customers_df['C_PHONE'].str[:2]
customers_df = customers_df[customers_df['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21'])]
customers_df = customers_df[customers_df['C_ACCTBAL'] > 0]

avg_acct_bal = customers_df.groupby('CNTRYCODE')['C_ACCTBAL'].mean().reset_index()
avg_acct_bal.columns = ['CNTRYCODE', 'AVG_ACCTBAL']

# Process Redis data
orders_df = get_redis_orders()
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
date_threshold = datetime.now() - timedelta(days=7*365)

active_customers = orders_df[orders_df['O_ORDERDATE'] > date_threshold]['O_CUSTKEY'].unique()

# Filter out active customers
customers_no_order_7_years = customers_df[~customers_df['C_CUSTKEY'].isin(active_customers)]

# Result dataset
result = customers_no_order_7_years.merge(avg_acct_bal, on='CNTRYCODE')
result = result[result['C_ACCTBAL'] > result['AVG_ACCTBAL']]

final_result = result.groupby('CNTRYCODE').agg(
    Number_of_Customers=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    Total_Account_Balance=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index().sort_values('CNTRYCODE')

# Write to CSV
final_result.to_csv('query_output.csv', index=False)
