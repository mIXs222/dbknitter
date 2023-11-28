import pandas as pd
import direct_redis

def main():
    # Establish connection to Redis using direct_redis
    connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    
    # Retrieve tables as Pandas DataFrames
    orders_df = pd.read_json(connection.get("orders"))
    lineitem_df = pd.read_json(connection.get("lineitem"))
    
    # Convert date strings to datetime objects
    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
    lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
    lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
    
    # Filter timestamps
    start_date = '1993-07-01'
    end_date = '1993-10-01'
    filtered_orders = orders_df[(orders_df['O_ORDERDATE'] >= start_date) & (orders_df['O_ORDERDATE'] <= end_date)]
    
    # Merge orders with lineitem on L_ORDERKEY = O_ORDERKEY
    merged_df = pd.merge(filtered_orders, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    
    # Apply condition L_COMMITDATE < L_RECEIPTDATE
    valid_orders = merged_df[merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']]
    
    # Group by O_ORDERPRIORITY and count valid orders
    final_counts = valid_orders.groupby('O_ORDERPRIORITY').size().reset_index(name='count')
    
    # Sort by O_ORDERPRIORITY
    final_counts_sorted = final_counts.sort_values(by='O_ORDERPRIORITY')
    
    # Write to CSV file
    final_counts_sorted.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
