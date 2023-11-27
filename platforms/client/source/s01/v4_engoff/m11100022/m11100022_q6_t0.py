import pandas as pd
from direct_redis import DirectRedis

# Function to filter and calculate the increased revenue
def forecast_revenue(df):
    # Filter the DataFrame for given conditions
    filtered_df = df[
        (df['L_SHIPDATE'] >= '1994-01-01') &
        (df['L_SHIPDATE'] < '1995-01-01') &
        (df['L_DISCOUNT'] >= 0.06 - 0.01) &
        (df['L_DISCOUNT'] <= 0.06 + 0.01) &
        (df['L_QUANTITY'] < 24)
    ]
    # Calculate the increased revenue
    filtered_df['REVENUE_INCREASE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']
    return filtered_df['REVENUE_INCREASE'].sum()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve the lineitem table from Redis
lineitem_data = redis_client.get('lineitem')  # Assuming this returns a JSON or string representation of the data

# Convert the lineitem data into a DataFrame
lineitem_df = pd.read_json(lineitem_data)

# Compute the result
total_revenue_increase = forecast_revenue(lineitem_df)

# Write the result to query_output.csv
with open('query_output.csv', 'w') as f:
    f.write(f"TOTAL_REVENUE_INCREASE\n{total_revenue_increase}\n")
