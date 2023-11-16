import pandas as pd
from direct_redis import DirectRedis

def fetch_table(redis_client, table_name):
    return pd.DataFrame(redis_client.get(table_name))

def main():
    # Create a connection to the Redis database
    redis_client = DirectRedis(host='redis', port=6379, db=0)

    # Fetch the tables from Redis
    nation_df = fetch_table(redis_client, 'nation')
    region_df = fetch_table(redis_client, 'region')
    supplier_df = fetch_table(redis_client, 'supplier')
    customer_df = fetch_table(redis_client, 'customer')
    orders_df = fetch_table(redis_client, 'orders')
    lineitem_df = fetch_table(redis_client, 'lineitem')

    # Convert dates from strings to datetime objects for comparison
    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

    # Join the tables based on the provided query conditions
    merged_df = (
        customer_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
        .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
        .merge(supplier_df, left_on=['C_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY'])
        .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
        .merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
    )

    # Date condition
    date_condition = (merged_df['O_ORDERDATE'] >= '1990-01-01') & (merged_df['O_ORDERDATE'] < '1995-01-01')

    # Filter the dataframe based on the query conditions
    filtered_df = merged_df[
        (merged_df['R_NAME'] == 'ASIA') &
        date_condition
    ]

    # Calculate revenue for each group
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

    # Group by nation name and sum the revenue
    result_df = filtered_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

    # Sort the results as in the query
    result_df = result_df.sort_values('REVENUE', ascending=False)

    # Write the result to a CSV file
    result_df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
