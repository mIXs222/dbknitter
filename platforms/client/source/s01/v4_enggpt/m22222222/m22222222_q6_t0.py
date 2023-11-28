import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Function to convert string dates in format 'YYYY-MM-DD' to datetime objects
def str_to_datetime(s):
    return datetime.strptime(s, '%Y-%m-%d')

# Function to filter and calculate revenue
def calculate_revenue_from_lineitem(dataframe):
    # Filter records with shipping dates in specified range
    dataframe['L_SHIPDATE'] = dataframe['L_SHIPDATE'].apply(str_to_datetime)
    filtered_df = dataframe[
        (dataframe['L_SHIPDATE'] >= datetime(1994, 1, 1)) &
        (dataframe['L_SHIPDATE'] <= datetime(1994, 12, 31)) &
        (dataframe['L_DISCOUNT'] >= 0.06 - 0.01) &
        (dataframe['L_DISCOUNT'] <= 0.06 + 0.01) &
        (dataframe['L_QUANTITY'] < 24)
    ]
    
    # Compute the total revenue
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
    total_revenue = filtered_df['REVENUE'].sum()
    return total_revenue

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data as Pandas DataFrame
lineitem_df = redis_client.get('lineitem')

# Calculate revenue
total_revenue = calculate_revenue_from_lineitem(lineitem_df)

# Write results to CSV
pd.DataFrame({'Total Revenue': [total_revenue]}).to_csv('query_output.csv', index=False)
