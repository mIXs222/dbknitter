from pymongo import MongoClient
import pandas as pd
import redis
from datetime import datetime, timedelta
import csv
import direct_redis

# MongoDB connection and query
def get_mongodb_orders_data():
    # MongoDB connection
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    orders_collection = db['orders']

    # Find orders placed within the last 7 years to exclude in the Redis query
    seven_years_ago = datetime.now() - timedelta(days=7*365)
    recent_orders = list(orders_collection.find(
        {"O_ORDERDATE": {"$gt": seven_years_ago.strftime('%Y-%m-%d')}},
        {"O_CUSTKEY": 1, "_id": 0}
    ))
    # Flatten the list of customer keys
    recent_orders_custkeys = [order['O_CUSTKEY'] for order in recent_orders]
    client.close()
    return recent_orders_custkeys

# Redis connection and query
def get_redis_customers_data(recent_orders_custkeys):
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    # Get all customers as dataframe
    df_customers = r.get('customer')
    if df_customers is None:
        return pd.DataFrame()  # Return an empty DataFrame if no data is found
    
    # Filter customers by country code and account balance > 0
    country_codes = {'20', '40', '22', '30', '39', '42', '21'}
    df_customers = df_customers[df_customers.C_PHONE.str[:2].isin(country_codes)]
    df_customers = df_customers[df_customers.C_ACCTBAL > 0]

    # Calculate average account balance across these customers
    average_balance = df_customers['C_ACCTBAL'].mean()

    # Further filter by customers who have not placed an order in the last 7 years and balance > average
    df_customers = df_customers[~df_customers['C_CUSTKEY'].isin(recent_orders_custkeys)]
    df_customers = df_customers[df_customers.C_ACCTBAL > average_balance]

    # Aggregate customers by country code
    df_agg = df_customers.groupby(df_customers.C_PHONE.str[:2]) \
                          .agg({'C_CUSTKEY': 'count', 'C_ACCTBAL': 'sum'}) \
                          .rename(columns={'C_CUSTKEY': 'NUM_CUSTOMERS', 'C_ACCTBAL': 'TOTAL_BALANCE'}) \
                          .sort_index().reset_index() \
                          .rename(columns={'C_PHONE': 'CNTRYCODE'})

    return df_agg

def main():
    recent_orders_custkeys = get_mongodb_orders_data()
    result_df = get_redis_customers_data(recent_orders_custkeys)
    # Write output to CSV file
    result_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
