# query.py

import pandas as pd
import direct_redis

def get_redis_table(hostname, port, database_name, table_name):
    r = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)
    table_json = r.get(table_name)
    return pd.read_json(table_json)

def main():
    # Connect to Redis and get datasets
    redis_hostname = 'redis'
    redis_port = 6379
    redis_db = 0

    orders_df = get_redis_table(redis_hostname, redis_port, redis_db, 'orders')
    lineitem_df = get_redis_table(redis_hostname, redis_port, redis_db, 'lineitem')

    # Convert columns to datetime
    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
    lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
    lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

    # Filter based on the timeframe and condition
    orders_timeframe = orders_df[(orders_df['O_ORDERDATE'] >= '1993-07-01') & (orders_df['O_ORDERDATE'] <= '1993-10-01')]
    lineitem_condition_met = lineitem_df[lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']]
    
    # Join the orders and lineitem tables on L_ORDERKEY = O_ORDERKEY
    valid_orders_with_lineitem = pd.merge(orders_timeframe, lineitem_condition_met, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Group by order priority and count unique orders
    orders_count = valid_orders_with_lineitem.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index()

    # Rename the columns to match the expected output
    orders_count.columns = ['Order Priority', 'Count']

    # Sort by order priority
    orders_count.sort_values('Order Priority', inplace=True)

    # Write the results to a CSV file
    orders_count.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
