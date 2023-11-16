# Python code (query_redis.py)
import pandas as pd
from direct_redis import DirectRedis

def query_redis_and_save():
    # Connection to the Redis database
    connection_parameters = {
        'host': 'redis',
        'port': 6379,
        'db': 0,
    }
    redis_instance = DirectRedis(**connection_parameters)

    # Retrieve tables from Redis
    customer_df = pd.read_json(redis_instance.get('customer'))
    orders_df = pd.read_json(redis_instance.get('orders'))
    lineitem_df = pd.read_json(redis_instance.get('lineitem'))

    # Perform the SQL-like operations using pandas
    # Filter customers by MKTSEGMENT
    customer_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

    # Merge tables on keys
    merged_df = customer_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    merged_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Apply date filters
    merged_df = merged_df[(merged_df['O_ORDERDATE'] < '1995-03-15') & (merged_df['L_SHIPDATE'] > '1995-03-15')]

    # Calculate REVENUE and perform GROUP BY and ORDER BY operations
    merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
    
    result_df = merged_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']) \
        .agg({'REVENUE': 'sum'}) \
        .reset_index() \
        .sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

    # Save the result to a CSV file
    result_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    query_redis_and_save()
