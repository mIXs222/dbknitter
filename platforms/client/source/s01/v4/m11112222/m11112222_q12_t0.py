# query_redis.py
import pandas as pd
from direct_redis import DirectRedis

def connect_redis(host, port, db_name):
    return DirectRedis(host=host, port=port, db=db_name)

def main():
    redis_connection = connect_redis('redis', 6379, '0')
    
    # Read the tables as DataFrames via the custom DirectRedis class
    df_orders = pd.read_json(redis_connection.get('orders'))
    df_lineitem = pd.read_json(redis_connection.get('lineitem'))
    
    # Perform the join and the filtering operations
    merged_df = df_orders.merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    filtered_df = merged_df[
        (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
        (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) &
        (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) &
        (merged_df['L_RECEIPTDATE'] >= '1994-01-01') &
        (merged_df['L_RECEIPTDATE'] < '1995-01-01')
    ]

    # Perform the aggregation
    aggregated_df = filtered_df.groupby('L_SHIPMODE').apply(
        lambda g: pd.Series({
            'HIGH_LINE_COUNT': g[g['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])].shape[0],
            'LOW_LINE_COUNT': g[~g['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])].shape[0],
        })
    ).reset_index()

    # Write the result to a CSV file
    aggregated_df.to_csv('query_output.csv', index=False)


if __name__ == '__main__':
    main()
