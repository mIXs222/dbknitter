import pandas as pd
from direct_redis import DirectRedis

def query_redis_data():
    # Connect to redis database
    redis_host = 'redis'
    redis_port = 6379
    redis_db = 0
    redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

    # Read lineitem DataFrame from Redis
    lineitem_key = 'lineitem'
    lineitem_df = pd.read_msgpack(redis_client.get(lineitem_key))

    # Query
    result_df = lineitem_df[
        (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
        (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
        (lineitem_df['L_DISCOUNT'] >= (.06 - 0.01)) &
        (lineitem_df['L_DISCOUNT'] <= (.06 + 0.01)) &
        (lineitem_df['L_QUANTITY'] < 24)
    ]

    # Calculate REVENUE
    result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * result_df['L_DISCOUNT']

    # Summarize the result
    revenue_sum = result_df['REVENUE'].sum()

    # Write result to a csv file
    with open('query_output.csv', 'w') as file:
        file.write('REVENUE\n')
        file.write(str(revenue_sum))

if __name__ == "__main__":
    query_redis_data()
