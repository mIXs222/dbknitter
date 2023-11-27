import pandas as pd
from direct_redis import DirectRedis

def main():
    # Initialize connection to Redis
    redis_conn = DirectRedis(host='redis', port=6379, db=0)

    # Read data from Redis into Pandas DataFrame
    df_customers = pd.read_msgpack(redis_conn.get('customer'))
    df_orders = pd.read_msgpack(redis_conn.get('orders'))

    # Filter out orders with statuses of "pending" or those including "deposits" in the comments
    df_orders_filtered = df_orders[~df_orders['O_ORDERSTATUS'].str.lower().str.contains('pending') & 
                                   ~df_orders['O_COMMENT'].str.lower().str.contains('deposits')]
    
    # Count the number of orders by each customer
    orders_count = df_orders_filtered.groupby('O_CUSTKEY')['O_ORDERKEY'].nunique().reset_index()
    orders_count.columns = ['C_CUSTKEY', 'TotalOrders']

    # Merge the customer list with the order counts
    df_result = pd.merge(df_customers, orders_count, how='left', left_on='C_CUSTKEY', right_on='C_CUSTKEY')

    # Replace NaN in TotalOrders with 0
    df_result['TotalOrders'].fillna(0, inplace=True)

    # Count the distribution of number of orders by customers including those with zero orders
    order_distribution = df_result.groupby('TotalOrders').size().reset_index(name='CustomerCount')

    # Write the results to a CSV file
    order_distribution.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
