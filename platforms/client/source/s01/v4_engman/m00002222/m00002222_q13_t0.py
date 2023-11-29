import pandas as pd
from direct_redis import DirectRedis

def main():
    # Connect to Redis database
    redis_host = "redis"
    redis_port = 6379
    redis_db = 0

    redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db)
    
    # Retrieve data from Redis tables as Pandas DataFrames
    customer_df = pd.read_json(redis_client.get('customer'))
    orders_df = pd.read_json(redis_client.get('orders'))
    
    # Filter orders that are not pending or do not fall into deposits
    special_orders_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', regex=True)]
    
    # Merge customers and special orders on customer key
    merged_df = pd.merge(customer_df, special_orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')
    
    # Group by customer and count orders
    orders_per_customer = merged_df.groupby('C_CUSTKEY').size().reset_index(name='orders_count')
    
    # Group by the number of orders to get the distribution of customers
    customer_distribution = orders_per_customer.groupby('orders_count').size().reset_index(name='num_customers')
    
    # Write the results to CSV file
    customer_distribution.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
