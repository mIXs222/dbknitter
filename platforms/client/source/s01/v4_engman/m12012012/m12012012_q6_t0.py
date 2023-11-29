import pandas as pd
import direct_redis
import datetime

def query_redis():
    # Connect to the Redis server
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    
    # Read data from Redis
    lineitem_df = r.get('lineitem')
    
    # Convert into a DataFrame
    lineitem_df = pd.read_json(lineitem_df)
    
    # Convert string dates to datetime
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
    
    # Filter based on query conditions
    filtered_df = lineitem_df[
        (lineitem_df['L_SHIPDATE'] > datetime.datetime(1994, 1, 1)) &
        (lineitem_df['L_SHIPDATE'] < datetime.datetime(1995, 1, 1)) &
        (lineitem_df['L_DISCOUNT'] > 0.06 - 0.01) &
        (lineitem_df['L_DISCOUNT'] < 0.06 + 0.01) &
        (lineitem_df['L_QUANTITY'] < 24)
    ]
    
    # Calculate revenue
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']
    
    # Sum up revenue
    result = filtered_df['REVENUE'].sum()
    
    # Create resulting DataFrame with one column 'REVENUE'
    result_df = pd.DataFrame({'REVENUE': [result]})
    
    # Write result to file
    result_df.to_csv('query_output.csv', index=False)

# Execute the query
query_redis()
