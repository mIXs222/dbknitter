import pandas as pd
import direct_redis

def is_valid_country(phone, valid_codes):
    return phone[:2] in valid_codes

def read_dataframe_from_redis(table_name):
    redis_db = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    df_json = redis_db.get(table_name)
    if df_json:
        return pd.read_json(df_json, orient='records')
    return None

def main():
    valid_country_codes = ['20', '40', '22', '30', '39', '42', '21']

    # Read data from Redis
    customers_df = read_dataframe_from_redis('customer')
    orders_df = read_dataframe_from_redis('orders')

    # Filter customers based on phone and account balance
    filtered_customers_df = customers_df[
        customers_df.apply(lambda x: is_valid_country(x['C_PHONE'], valid_country_codes) 
                           and x['C_ACCTBAL'] > 0.00,
                           axis=1)
    ]

    # Find customers who have not placed an order in the last 7 years
    seven_years_ago = pd.Timestamp.now() - pd.DateOffset(years=7)
    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
    recent_orders_df = orders_df[orders_df['O_ORDERDATE'] >= seven_years_ago]
    # Get customer keys of those who have placed orders in the last 7 years
    recent_customers = recent_orders_df['O_CUSTKEY'].unique()
    
    # Remove customers who have placed recent orders
    targeted_customers_df = filtered_customers_df[~filtered_customers_df['C_CUSTKEY'].isin(recent_customers)]

    # Group by the country code with count of customers and average balance
    targeted_customers_df['COUNTRY_CODE'] = targeted_customers_df['C_PHONE'].str[:2]
    result = targeted_customers_df.groupby('COUNTRY_CODE').agg({'C_CUSTKEY': 'count', 'C_ACCTBAL': 'mean'}).reset_index()

    # Rename columns to match required output
    result.columns = ['COUNTRY_CODE', 'CUSTOMER_COUNT', 'AVERAGE_BALANCE']

    # Write result to file
    result.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
