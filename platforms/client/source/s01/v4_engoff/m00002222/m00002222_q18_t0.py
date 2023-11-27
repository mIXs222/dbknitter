import pandas as pd
from direct_redis import DirectRedis

def query_large_volume_customers():
    # Establish a connection to the Redis database
    redis_client = DirectRedis(host='redis', port=6379, db=0)

    # Get the dataframes from Redis
    df_customers = pd.read_json(redis_client.get('customer'))
    df_orders = pd.read_json(redis_client.get('orders'))
    df_lineitem = pd.read_json(redis_client.get('lineitem'))

    # Merge dataframes to get necessary information
    df_merge = pd.merge(df_customers, df_orders, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    df_merge = pd.merge(df_merge, df_lineitem, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Calculate total quantity per order and filter orders greater than 300
    df_large_volume = df_merge.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']) \
    .agg({'L_QUANTITY':'sum'}).reset_index()
    df_large_volume = df_large_volume[df_large_volume['L_QUANTITY'] > 300]

    # Select required columns
    df_result = df_large_volume[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

    # Write the results to a CSV file
    df_result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    query_large_volume_customers()
