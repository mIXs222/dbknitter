# query.py
import pandas as pd
from direct_redis import DirectRedis

def main():
    # Establish a connection to the Redis database
    redis_conn = DirectRedis(host='redis', port=6379, db=0)

    # Read data from Redis into Pandas DataFrames
    orders_df = pd.read_json(redis_conn.get('orders'))
    lineitem_df = pd.read_json(redis_conn.get('lineitem'))

    # Convert string dates to datetime
    lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
    lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

    # Filter lineitems within the specified date range
    filtered_lineitems = lineitem_df[(lineitem_df['L_RECEIPTDATE'] >= '1994-01-01') & (lineitem_df['L_RECEIPTDATE'] <= '1995-01-01')]
    
    # Join with the orders table to get order priority
    joined_data = filtered_lineitems.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    
    # Filter out records where lineitem ship date is after commit date
    shipped_before_commit = joined_data[joined_data['L_SHIPDATE'] < joined_data['L_COMMITDATE']]
    
    # Check for receipt date being after commit date and shipping mode condition
    late_shipments = shipped_before_commit[(shipped_before_commit['L_RECEIPTDATE'] > shipped_before_commit['L_COMMITDATE']) &
                                           (shipped_before_commit['L_SHIPMODE'].isin(['MAIL', 'SHIP']))]

    # Split into groups based on order priority
    late_high_priority = late_shipments[late_shipments['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH'])]
    late_other_priority = late_shipments[~late_shipments['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH'])]

    # Get counts by ship mode
    late_high_priority_count = late_high_priority.groupby('L_SHIPMODE').size().reset_index(name='High_Priority_Count')
    late_other_priority_count = late_other_priority.groupby('L_SHIPMODE').size().reset_index(name='Other_Priority_Count')

    # Merge the results
    result = pd.merge(late_high_priority_count, late_other_priority_count, on='L_SHIPMODE', how='outer').fillna(0)

    # Write the output to a CSV file
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
