import pandas as pd
from direct_redis import DirectRedis

def forecast_revenue_change():
    # Connect to Redis
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    
    # Read the lineitem data into a DataFrame
    lineitem_df = pd.read_json(redis_client.get('lineitem'))
    
    # Convert the SHIPDATE to DateTime
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
    
    # Filter lineitems based on the given conditions
    filtered_lineitems = lineitem_df[
        (lineitem_df['L_SHIPDATE'] >= pd.Timestamp('1994-01-01')) &
        (lineitem_df['L_SHIPDATE'] < pd.Timestamp('1995-01-01')) &
        (lineitem_df['L_DISCOUNT'] >= 0.06 - 0.01) &
        (lineitem_df['L_DISCOUNT'] <= 0.06 + 0.01) &
        (lineitem_df['L_QUANTITY'] < 24)
    ]
    
    # Calculate the potential revenue increase
    potential_revenue_increase = (
        filtered_lineitems['L_EXTENDEDPRICE'] * filtered_lineitems['L_DISCOUNT']
    ).sum()

    # Write the result to a CSV file
    pd.DataFrame({'Potential Revenue Increase': [potential_revenue_increase]}).to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    forecast_revenue_change()
