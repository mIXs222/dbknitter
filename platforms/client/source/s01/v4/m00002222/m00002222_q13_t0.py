import pandas as pd
from direct_redis import DirectRedis

def connect_to_redis(hostname, port, db_name):
    return DirectRedis(host=hostname, port=port, db=db_name)
    
def fetch_data_from_redis(redis_connection, table_name):
    data_str = redis_connection.get(table_name)
    if data_str:
        data_df = pd.read_json(data_str, orient='records')
        return data_df
    return None

def main():
    # Connect to Redis
    redis_conn = connect_to_redis('redis', 6379, '0')

    # Fetch data from Redis
    customer_df = fetch_data_from_redis(redis_conn, 'customer')
    orders_df = fetch_data_from_redis(redis_conn, 'orders')

    # Filtering orders without 'pending%deposits' in comments
    orders_filtered_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending%deposits', regex=False)]

    # Perform LEFT OUTER JOIN
    merged_df = pd.merge(customer_df, orders_filtered_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    
    # Group by C_CUSTKEY and count the number of orders for each customer
    customer_order_count = merged_df.groupby('C_CUSTKEY')['O_ORDERKEY'].count().reset_index(name='C_COUNT')
    
    # Group by C_COUNT and count the number of customers for each count of orders
    count_distribution = customer_order_count.groupby('C_COUNT')['C_CUSTKEY'].count().reset_index(name='CUSTDIST')

    # Sort the results
    count_distribution_sorted = count_distribution.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

    # Output to CSV
    count_distribution_sorted.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
