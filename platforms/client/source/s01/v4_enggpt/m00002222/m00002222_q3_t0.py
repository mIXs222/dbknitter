# query.py
import pandas as pd
import direct_redis

def calculate_revenue_from_redis():
    # Connect to Redis
    db = direct_redis.DirectRedis(hostname='redis', port=6379, db=0)

    # Load dataframes from Redis
    customer_df = pd.read_json(db.get('customer'))
    orders_df = pd.read_json(db.get('orders'))
    lineitem_df = pd.read_json(db.get('lineitem'))

    # Filter dataframes based on query conditions
    customer_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

    orders_before = orders_df[orders_df['O_ORDERDATE'] < '1995-03-15']
    lineitems_after = lineitem_df[lineitem_df['L_SHIPDATE'] > '1995-03-15']

    # Merge dataframes on keys
    merged_df = customer_df.merge(orders_before, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    merged_df = merged_df.merge(lineitems_after, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Apply discount and compute revenue
    merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

    # Group by the required fields
    grouped_results = merged_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])['REVENUE'].sum().reset_index()

    # Sort by revenue descending and order date ascending
    final_results = grouped_results.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

    # Write results to CSV
    final_results.to_csv('query_output.csv', index=False)

calculate_revenue_from_redis()
