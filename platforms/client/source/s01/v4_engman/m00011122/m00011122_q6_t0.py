# query.py
import pandas as pd
import direct_redis

def query_redis():
    # Connect to Redis using DirectRedis
    connection_params = {
        'host': 'redis',
        'port': 6379,
        'db': 0
    }
    client = direct_redis.DirectRedis(**connection_params)

    # Get lineitem table from Redis
    lineitem_json = client.get('lineitem')
    lineitem_df = pd.read_json(lineitem_json)

    # Filter the DataFrame according to the criteria
    filtered_df = lineitem_df[
        (lineitem_df['L_SHIPDATE'] > '1994-01-01') &
        (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
        (lineitem_df['L_DISCOUNT'] >= 0.06 - 0.01) &
        (lineitem_df['L_DISCOUNT'] <= 0.06 + 0.01) &
        (lineitem_df['L_QUANTITY'] < 24)
    ]

    # Calculate revenue and sum it up
    revenue = (filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']).sum()

    # Create a DataFrame to store results and write it to CSV
    result_df = pd.DataFrame({'REVENUE': [revenue]})
    result_df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    query_redis()
