import pandas as pd
import direct_redis
import csv
from datetime import datetime

def get_redis_data(hostname, port, database_name, table):
    redis_connection = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)
    data = redis_connection.get(table)
    return pd.read_json(data)

def main():
    # Redis connection info
    hostname = 'redis'
    port = 6379
    database_name = 0
    
    # Get data from Redis
    orders_df = get_redis_data(hostname, port, database_name, 'orders')
    lineitem_df = get_redis_data(hostname, port, database_name, 'lineitem')

    # Convert strings to datetime for comparison
    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
    lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
    lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

    # Filter orders within the date range
    start_date = datetime(1993, 7, 1)
    end_date = datetime(1993, 10, 1)
    orders_df = orders_df[(orders_df['O_ORDERDATE'] >= start_date) & (orders_df['O_ORDERDATE'] <= end_date)]

    # Filter lineitems where commit date is before receipt date
    lineitem_df = lineitem_df[lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']]

    # Merge orders and lineitems dataframes on the order key where they match
    merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Get counts of orders based on priority with the conditions
    order_priority_counts = merged_df.groupby('O_ORDERPRIORITY').size().reset_index(name='count')

    # Sort by order priority
    order_priority_counts_sorted = order_priority_counts.sort_values(by='O_ORDERPRIORITY')

    # Write the result to CSV
    order_priority_counts_sorted.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
