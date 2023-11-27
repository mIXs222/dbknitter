# query.py
import pandas as pd
from direct_redis import DirectRedis

def redis_to_dataframe(redis_instance, table_name):
    data = redis_instance.get(table_name)
    if data is None:
        raise ValueError(f"Table {table_name} does not exist in Redis.")
    df = pd.read_json(data)
    return df

def main():
    # Connect to Redis
    redis_instance = DirectRedis(host='redis', port=6379, db=0)

    # Fetch tables from Redis and convert to DataFrames
    orders_df = redis_to_dataframe(redis_instance, 'orders')
    lineitem_df = redis_to_dataframe(redis_instance, 'lineitem')

    # Convert date strings to datetime
    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
    lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
    lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

    # Merge DataFrames on the order key
    merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Filter based on given conditions
    filtered_df = merged_df[
        (merged_df['L_RECEIPTDATE'].between('1994-01-01', '1995-01-01')) & 
        (merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE']) & 
        (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) &
        (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP']))
    ]

    # Group by ship mode and order priority
    result = filtered_df.assign(
        PRIORITY_GROUP=pd.Categorical(
            np.where(
                filtered_df['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH']), 
                'URGENT/HIGH', 
                'OTHER'
            ),
            categories=['URGENT/HIGH', 'OTHER'],
            ordered=True
        )
    ).groupby(['L_SHIPMODE', 'PRIORITY_GROUP']).size().reset_index(name='COUNT')

    # Sort the results
    result = result.sort_values(by=['L_SHIPMODE', 'PRIORITY_GROUP'])

    # Write results to CSV
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
