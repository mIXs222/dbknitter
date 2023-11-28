import pandas as pd
from direct_redis import DirectRedis
import datetime

def main():
    # Connect to Redis
    hostname = "redis"
    port = 6379
    dr = DirectRedis(hostname=hostname, port=port, db=0)

    # Get 'lineitem' table data
    lineitem_df = pd.read_json(dr.get('lineitem'))

    # Convert string dates to datetime objects
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

    # Define the date range for filter
    start_date = datetime.datetime(1994, 1, 1)
    end_date = datetime.datetime(1994, 12, 31)

    # Apply filtering criteria
    filtered_df = lineitem_df[
        (lineitem_df['L_SHIPDATE'] >= start_date) &
        (lineitem_df['L_SHIPDATE'] <= end_date) &
        (lineitem_df['L_DISCOUNT'] >= 0.06 - 0.01) &
        (lineitem_df['L_DISCOUNT'] <= 0.06 + 0.01) &
        (lineitem_df['L_QUANTITY'] < 24)
    ]

    # Calculate extended price * (1 - discount)
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

    # Compute total revenue
    total_revenue = filtered_df['REVENUE'].sum()

    # Output results to CSV
    pd.DataFrame({'Total Revenue': [total_revenue]}).to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
