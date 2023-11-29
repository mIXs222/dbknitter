import pandas as pd
import direct_redis

def query_redis_data(hostname, port, db_name):
    # Connecting to the redis database
    dr = direct_redis.DirectRedis(host=hostname, port=port, db=db_name)
    
    # Reading the tables as pandas DataFrames
    customer_df = pd.read_json(dr.get('customer'), orient='records')
    orders_df = pd.read_json(dr.get('orders'), orient='records')
    lineitem_df = pd.read_json(dr.get('lineitem'), orient='records')
    
    return customer_df, orders_df, lineitem_df

def execute_query_and_save(customer_df, orders_df, lineitem_df):
    # Merge the tables to combine the information
    merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    merged_df = pd.merge(merged_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Filter the orders with the total quantity larger than 300
    orders_grouped = merged_df.groupby('O_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
    large_orders = orders_grouped[orders_grouped['L_QUANTITY'] > 300]
    
    # Final query result
    result = pd.merge(large_orders, merged_df, on='O_ORDERKEY')
    
    # Selecting required columns and sorting
    result = result[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
    result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)
    
    # Save to CSV
    result.to_csv('query_output.csv', index=False)

def main():
    # Configuration
    hostname = 'redis'
    port = 6379
    db_name = '0'
    
    # Query data
    customer_df, orders_df, lineitem_df = query_redis_data(hostname, port, db_name)
    
    # Execute query and save to CSV
    execute_query_and_save(customer_df, orders_df, lineitem_df)

if __name__ == "__main__":
    main()
