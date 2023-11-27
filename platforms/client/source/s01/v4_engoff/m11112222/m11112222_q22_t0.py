import pandas as pd
import numpy as np
from direct_redis import DirectRedis

def get_data_from_redis(table_name):
    direct_redis = DirectRedis(host='redis', port=6379, db=0)
    table_data = direct_redis.get(table_name)
    if table_data is not None:
        df = pd.read_json(table_data)
    else:
        df = None
    return df

def query_global_sales_opportunity(customers, orders):
    # Filter for the required country codes in customer phone
    filtered_customers = customers[customers['C_PHONE'].str.slice(0, 2).isin(['20', '40', '22', '30', '39', '42', '21'])]

    # Get customers who have not placed orders for the last 7 years
    current_year = pd.Timestamp('now').year
    seven_years_ago = current_year - 7
    customers_no_recent_orders = filtered_customers[~filtered_customers['C_CUSTKEY'].isin(orders['O_CUSTKEY'][orders['O_ORDERDATE'].dt.year > seven_years_ago])]

    # Calculate the average account balance greater than 0
    customers_positive_balance = customers_no_recent_orders[customers_no_recent_orders['C_ACCTBAL'] > 0]

    # Get the country code from the phone number
    customers_positive_balance['CountryCode'] = customers_positive_balance['C_PHONE'].str.slice(0, 2)

    # Group by country code and evaluate count and average balance
    result = customers_positive_balance.groupby('CountryCode').agg(
        CustomerCount=('C_CUSTKEY', 'count'), 
        AverageBalance=('C_ACCTBAL', 'mean')
    ).reset_index()
    
    # Write to csv file
    result.to_csv('query_output.csv', index=False)

def main():
    # Get data from Redis
    customers = get_data_from_redis('customer')
    orders = get_data_from_redis('orders')

    # Process the orders DataFrame
    orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
    
    # Execute the query function
    query_global_sales_opportunity(customers, orders)

if __name__ == "__main__":
    main()
