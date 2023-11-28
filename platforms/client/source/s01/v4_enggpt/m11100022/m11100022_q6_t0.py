# query.py

import pandas as pd
import direct_redis

# Function to filter the line items based on the given criteria
def filter_lineitems(df):
    # Convert 'L_SHIPDATE' to datetime
    df['L_SHIPDATE'] = pd.to_datetime(df['L_SHIPDATE'])
    
    # Define date range
    start_date = pd.Timestamp(year=1994, month=1, day=1)
    end_date = pd.Timestamp(year=1994, month=12, day=31)
    
    # Apply filter conditions
    filtered_df = df[
        (df['L_SHIPDATE'] >= start_date) & 
        (df['L_SHIPDATE'] <= end_date) &
        (df['L_DISCOUNT'] >= 0.05) & 
        (df['L_DISCOUNT'] <= 0.07) &
        (df['L_QUANTITY'] < 24)
    ]
    
    # Calculate revenue
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
    
    # Aggregate total revenue
    total_revenue = filtered_df['REVENUE'].sum()
    
    return total_revenue

# Connect to the Redis database using provided information
redis_host = 'redis'
redis_port = 6379
redis_db = 0

# Establish a connection to the Redis database
connection = direct_redis.DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Get 'lineitem' table data as pandas DataFrame
lineitem_df = connection.get('lineitem')

# Filter the dataframe and calculate revenue
total_revenue = filter_lineitems(lineitem_df)

# Output the result to query_output.csv
output_df = pd.DataFrame({'Total_Revenue': [total_revenue]})
output_df.to_csv('query_output.csv', index=False)
