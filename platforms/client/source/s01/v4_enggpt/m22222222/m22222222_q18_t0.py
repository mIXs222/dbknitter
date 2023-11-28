import pandas as pd
from direct_redis import DirectRedis

def main():
    # Connect to the Redis database
    redis = DirectRedis(host='redis', port=6379, db=0)
    
    # Load data into Pandas DataFrames using redis' get function
    df_customer = pd.DataFrame(redis.get('customer'))
    df_orders = pd.DataFrame(redis.get('orders'))
    df_lineitem = pd.DataFrame(redis.get('lineitem'))
    
    # Convert columns to appropriate data types
    df_customer = df_customer.astype({'C_CUSTKEY': int})
    df_orders = df_orders.astype({'O_ORDERKEY': int, 'O_CUSTKEY': int, 'O_TOTALPRICE': float})
    df_lineitem = df_lineitem.astype({'L_ORDERKEY': int, 'L_QUANTITY': float})
    
    # Compute total quantity by order and filter orders with total quantity > 300
    order_quantities = df_lineitem.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
    large_orders = order_quantities[order_quantities['L_QUANTITY'] > 300]
    
    # Merge to get the full orders and customers for the filtered order keys
    merged_data = pd.merge(left=large_orders, right=df_orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    final_data = pd.merge(left=merged_data, right=df_customer, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    
    # Select relevant columns
    final_data = final_data[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
    
    # Group by the specified columns and sort as instructed
    final_result = final_data.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({'L_QUANTITY': 'sum'}).reset_index()
    final_result = final_result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])
    
    # Write the results to CSV
    final_result.to_csv('query_output.csv', index=False)


if __name__ == "__main__":
    main()
