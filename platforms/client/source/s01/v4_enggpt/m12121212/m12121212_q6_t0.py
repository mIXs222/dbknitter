import pandas as pd
import direct_redis
from datetime import datetime

def get_redis_dataframe(redis_client, table_name):
    return pd.read_json(redis_client.get(table_name), orient='records')

def generate_total_revenue(df):
    # Convert string dates to datetime objects
    df['L_SHIPDATE'] = pd.to_datetime(df['L_SHIPDATE'])
    
    # Set the date range for filtering
    start_date = datetime(1994, 1, 1)
    end_date = datetime(1994, 12, 31)

    # Apply filters: L_SHIPDATE, L_DISCOUNT and L_QUANTITY
    filtered_df = df[
        (df['L_SHIPDATE'] >= start_date) & 
        (df['L_SHIPDATE'] <= end_date) & 
        (df['L_DISCOUNT'] >= 0.05) & 
        (df['L_DISCOUNT'] <= 0.07) & 
        (df['L_QUANTITY'] < 24)
    ]
    
    # Calculate the total revenue
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
    total_revenue = filtered_df['REVENUE'].sum()

    # Output to .csv
    pd.DataFrame({'total_revenue': [total_revenue]}).to_csv('query_output.csv', index=False)

# Connection information
redis_host = 'redis'
redis_port = 6379
redis_db_name = 0

# Connect to the Redis database
redis_client = direct_redis.DirectRedis(host=redis_host, port=redis_port)

# Retrieve data from Redis and compute the total revenue
lineitem_df = get_redis_dataframe(redis_client, 'lineitem')
generate_total_revenue(lineitem_df)
