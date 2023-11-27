import pandas as pd
import direct_redis
import datetime

def forecast_revenue_change():
    # Set up the connection to the Redis database
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Get the lineitem table
    lineitem_df = r.get("lineitem")

    # Convert the data to a Pandas DataFrame
    lineitem_df = pd.DataFrame(lineitem_df)

    # Convert the relevant columns to numerical values and dates
    lineitem_df['L_QUANTITY'] = lineitem_df['L_QUANTITY'].astype(float)
    lineitem_df['L_EXTENDEDPRICE'] = lineitem_df['L_EXTENDEDPRICE'].astype(float)
    lineitem_df['L_DISCOUNT'] = lineitem_df['L_DISCOUNT'].astype(float)
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

    # Define the date range and discount range
    start_date = datetime.datetime(1994, 1, 1)
    end_date = datetime.datetime(1995, 1, 1)
    discount_lower_bound = 0.06 - 0.01
    discount_upper_bound = 0.06 + 0.01

    # Perform the query
    filtered_df = lineitem_df[
        (lineitem_df['L_SHIPDATE'] >= start_date) & 
        (lineitem_df['L_SHIPDATE'] < end_date) &
        (lineitem_df['L_DISCOUNT'] >= discount_lower_bound) & 
        (lineitem_df['L_DISCOUNT'] <= discount_upper_bound) &
        (lineitem_df['L_QUANTITY'] < 24)
    ]

    # Calculate the potential revenue increase
    filtered_df['revenue_increase'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']

    # Sum the revenue increase
    total_revenue_increase = filtered_df['revenue_increase'].sum()

    # Write the result to the output CSV file
    with open('query_output.csv', 'w') as outfile:
        outfile.write('total_revenue_increase\n')
        outfile.write(str(total_revenue_increase))

if __name__ == '__main__':
    forecast_revenue_change()
