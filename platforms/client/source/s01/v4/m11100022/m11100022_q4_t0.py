import pandas as pd
from direct_redis import DirectRedis

def main():
    redis = DirectRedis(host='redis', port=6379, db=0)

    # Fetch orders and lineitem tables from Redis
    orders_df = pd.read_json(redis.get('orders'), orient='records')
    lineitem_df = pd.read_json(redis.get('lineitem'), orient='records')

    # Filter orders based on date
    filtered_orders = orders_df[
        (orders_df['O_ORDERDATE'] >= '1993-07-01') &
        (orders_df['O_ORDERDATE'] < '1993-10-01')
    ]

    # Filter lineitem based on whether L_COMMITDATE < L_RECEIPTDATE
    lineitem_with_commit_before_receipt = lineitem_df[
        lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']
    ]

    # Make sure L_ORDERKEY is of the same type for both tables
    filtered_orders['O_ORDERKEY'] = filtered_orders['O_ORDERKEY'].astype(str)
    lineitem_with_commit_before_receipt['L_ORDERKEY'] = lineitem_with_commit_before_receipt['L_ORDERKEY'].astype(str)

    # Merge the tables based on the order key
    merged_df = pd.merge(
        filtered_orders,
        lineitem_with_commit_before_receipt,
        left_on='O_ORDERKEY',
        right_on='L_ORDERKEY',
        how='inner'
    )

    # Group by order priority and count
    result_df = merged_df.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

    # Sort results by order priority
    result_df.sort_values('O_ORDERPRIORITY', inplace=True)

    # Write output to query_output.csv
    result_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
