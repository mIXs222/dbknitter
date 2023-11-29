import pandas as pd
import direct_redis

def query_redis():
    # Connect to the Redis database
    redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Read the orders and lineitem tables
    orders_df = pd.read_json(redis_client.get('orders'), orient='records')
    lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

    # Convert columns to datetime for filtering
    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
    lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
    lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

    # Filter the orders within the date range
    filtered_orders = orders_df[(orders_df['O_ORDERDATE'] >= '1993-07-01') & 
                                (orders_df['O_ORDERDATE'] <= '1993-10-01')]

    # Filter lineitems where receipt date is after commit date
    late_lineitems = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]

    # Find orders with at least one late lineitem
    late_orders = filtered_orders[filtered_orders['O_ORDERKEY'].isin(late_lineitems['L_ORDERKEY'])]

    # Count such orders for each order priority
    order_priority_count = late_orders.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

    # Sort by order priority
    order_priority_count = order_priority_count.sort_values('O_ORDERPRIORITY')

    # Output the result to a CSV file
    order_priority_count.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    query_redis()
