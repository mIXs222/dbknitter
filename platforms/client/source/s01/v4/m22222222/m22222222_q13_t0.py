import pandas as pd
from direct_redis import DirectRedis

def execute_query_and_write_to_csv():
    # Establish connection to Redis
    r = DirectRedis(host='redis', port=6379, db=0)

    # Retrieve data from Redis
    customer_df = pd.read_json(r.get('customer'))
    orders_df = pd.read_json(r.get('orders'))

    # Perform left outer join
    merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

    # Filter out orders with 'pending%deposits' in O_COMMENT
    merged_df = merged_df[~merged_df.O_COMMENT.str.contains('pending%deposits%', na=False)]

    # Compute C_COUNT
    c_orders_df = merged_df.groupby('C_CUSTKEY')['O_ORDERKEY'].count().reset_index(name='C_COUNT')

    # Compute CUSTDIST
    custdist_df = c_orders_df.groupby('C_COUNT')['C_CUSTKEY'].count().reset_index(name='CUSTDIST')

    # Sort by CUSTDIST DESC, C_COUNT DESC
    custdist_df = custdist_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

    # Write result to CSV
    custdist_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query_and_write_to_csv()
