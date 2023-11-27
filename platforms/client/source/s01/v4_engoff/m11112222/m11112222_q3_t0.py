# python code to execute the shipping priority query on a Redis database.

import pandas as pd
from direct_redis import DirectRedis

def main():
    # Create a DirectRedis connection
    redis_conn = DirectRedis(host='redis', port=6379, db=0)
    
    # Read the tables from redis
    customer_df = pd.read_json(redis_conn.get('customer'))
    orders_df = pd.read_json(redis_conn.get('orders'))
    lineitem_df = pd.read_json(redis_conn.get('lineitem'))
    
    # Pre-process data (Ensure that the date is in correct format)
    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
    
    # Execute the shipping priority query
    merged_df = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')\
                         .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    
    # Filter rows based on the conditions
    filtered_df = merged_df[(merged_df['C_MKTSEGMENT'] == 'BUILDING') & 
                            (merged_df['O_ORDERDATE'] < '1995-03-15') & 
                            (merged_df['L_SHIPDATE'] > '1995-03-15')]
    
    # Calculate the revenue
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
    
    # Get orders with highest revenue
    max_revenue_df = filtered_df.sort_values(by='REVENUE', ascending=False).drop_duplicates(subset='O_ORDERKEY', keep='first')
    
    # Selecting required columns and renaming them for the output
    result_df = max_revenue_df[['O_ORDERKEY', 'O_SHIPPRIORITY', 'REVENUE']]
    result_df = result_df.rename(columns={'O_ORDERKEY': 'ORDERKEY', 'O_SHIPPRIORITY': 'SHIPPRIORITY'})
    
    # Sort the result based on the REVENUE in descending order
    result_df = result_df.sort_values(by='REVENUE', ascending=False)
    
    # Write the result to a CSV file
    result_df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
