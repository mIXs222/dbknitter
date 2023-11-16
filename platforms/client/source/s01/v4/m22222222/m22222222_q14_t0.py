import pandas as pd
import direct_redis

# Function to calculate promotion revenue
def calculate_promo_revenue(parts_df, lineitem_df):
    # Filter lineitem based on the date range
    lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1995-09-01') & (lineitem_df['L_SHIPDATE'] < '1995-10-01')]

    # Merge the dataframes
    merged_df = pd.merge(lineitem_df, parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Calculate the promotion revenue
    promo_sum = (merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])).sum()
    total_sum = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])).sum()
    promo_revenue = (promo_sum / total_sum) * 100 if total_sum != 0 else 0
    return promo_revenue

# Connection details for Redis
hostname = 'redis'
port = 6379
database_name = '0'

# Connect to Redis and read data into Pandas DataFrames
redis_connection = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)
part_df = pd.DataFrame(redis_connection.get('part'))
lineitem_df = pd.DataFrame(redis_connection.get('lineitem'))

# Perform query operation
promo_revenue = calculate_promo_revenue(part_df, lineitem_df)

# Output result to CSV file
result_df = pd.DataFrame({'PROMO_REVENUE': [promo_revenue]})
result_df.to_csv('query_output.csv', index=False)
