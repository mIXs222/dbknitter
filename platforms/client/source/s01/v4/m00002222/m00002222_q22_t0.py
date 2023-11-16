import pandas as pd
import direct_redis

def execute_query():
    # Make sure you have direct_redis available or installed
    conn_info = {
        'host': 'redis', 
        'port': 6379, 
        'db': 0
    }
    client = direct_redis.DirectRedis(**conn_info)

    # Fetching tables from Redis
    customer_df = client.get('customer')
    orders_df = client.get('orders')

    # Convert byte to pandas dataframe
    customer_df = pd.read_msgpack(customer_df)
    orders_df = pd.read_msgpack(orders_df)

    # Query logic start
    # Calculating average account balance from customers with positive balance
    avg_acctbal = customer_df[customer_df['C_ACCTBAL'] > 0.00]['C_ACCTBAL'].mean()

    # Filtering customers based on phone and account balance
    customer_filtered = customer_df[
        customer_df['C_PHONE'].str[:2].isin(['20', '40', '22', '30', '39', '42', '21']) &
        (customer_df['C_ACCTBAL'] > avg_acctbal)
    ]

    # Finding customers with no orders
    customer_no_orders = customer_filtered[
        ~customer_filtered['C_CUSTKEY'].isin(orders_df['O_CUSTKEY'])
    ]

    # Adding CNTRYCODE column
    customer_no_orders['CNTRYCODE'] = customer_no_orders['C_PHONE'].str[:2]

    # Group by CNTRYCODE and perform aggregation 
    result = customer_no_orders.groupby('CNTRYCODE').agg(
        NUMCUST=('C_CUSTKEY', 'count'),
        TOTACCTBAL=('C_ACCTBAL', 'sum')
    ).reset_index()

    # Sort result by CNTRYCODE
    result = result.sort_values(by='CNTRYCODE')

    # Query logic end

    # Write output to CSV file
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query()
