import pandas as pd
import direct_redis


def query_redis_data(host, port, db_name):
    # Connect to Redis
    redis_client = direct_redis.DirectRedis(host=host, port=port, db=db_name)
    
    # Read the data from Redis
    df_customer = pd.DataFrame(redis_client.get('customer'))
    df_orders = pd.DataFrame(redis_client.get('orders'))
    df_lineitem = pd.DataFrame(redis_client.get('lineitem'))
    
    # Parse the datasets into the correct data types
    for col in df_lineitem.select_dtypes(include=['object']).columns:
        df_lineitem[col] = pd.to_numeric(df_lineitem[col], errors='ignore')
    
    # Perform join operations
    df_cust_orders = df_customer.merge(df_orders, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    df_merged = df_cust_orders.merge(df_lineitem, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    
    # Aggregate by orders and filter by total quantity larger than 300
    df_grouped = df_merged.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'], as_index=False).agg({'L_QUANTITY': 'sum'})
    df_large_qty = df_grouped[df_grouped['L_QUANTITY'] > 300]
    
    # Keep only the required columns
    df_final = df_large_qty[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
    
    return df_final

# Query data from Redis
redis_data = query_redis_data(host='redis', port=6379, db_name=0)

# Output to CSV file
redis_data.to_csv('query_output.csv', index=False)
