import pandas as pd
from direct_redis import DirectRedis

def calculate_revenue(df_orders, df_lineitem, df_customer, df_nation):
    # Merge the dataframes based on the query conditions
    df_merge = df_orders.merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')\
                        .merge(df_customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')\
                        .merge(df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

    # Filter the merged dataframe as per the query conditions
    df_filtered = df_merge[(df_merge['O_ORDERDATE'] >= '1993-10-01')
                           & (df_merge['O_ORDERDATE'] < '1994-01-01')
                           & (df_merge['L_RETURNFLAG'] == 'R')]

    # Calculate REVENUE and group by required columns
    df_filtered['REVENUE'] = df_filtered['L_EXTENDEDPRICE'] * (1 - df_filtered['L_DISCOUNT'])
    df_grouped = df_filtered.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'])\
                            .agg({'REVENUE': 'sum'})

    # Reset index to flatten the grouped dataframe
    df_grouped = df_grouped.reset_index()

    # Sort the dataframe as per the query's ORDER BY conditions
    df_grouped.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False], inplace=True)

    # Return the prepared dataframe
    return df_grouped

def main():
    # Create a connection to the Redis database
    redis_connection = DirectRedis(host='redis', port=6379, db=0)
    
    # Load tables from Redis into Pandas DataFrames
    nation_df = pd.read_json(redis_connection.get('nation'))
    customer_df = pd.read_json(redis_connection.get('customer'))
    orders_df = pd.read_json(redis_connection.get('orders'))
    lineitem_df = pd.read_json(redis_connection.get('lineitem'))

    # Calculate revenue and get the result
    result_df = calculate_revenue(orders_df, lineitem_df, customer_df, nation_df)

    # Write result to CSV
    result_df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
