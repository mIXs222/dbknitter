# query.py
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Load Redis data into DataFrames
supplier_df = pd.read_json(r.get('supplier'))
lineitem_df = pd.read_json(r.get('lineitem'))

# Parse dates for lineitem DataFrame
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Define the date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)

# Create the 'revenue0' view equivalent
revenue0 = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)
].assign(
    Revenue=lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])
).groupby('L_SUPPKEY').agg(
    Total_Revenue=pd.NamedAgg(column='Revenue', aggfunc='sum')
).reset_index()

# Join with the suppliers on supplier key
result = pd.merge(supplier_df, revenue0, how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Find the supplier with the maximum revenue
max_revenue_supplier = result[result['Total_Revenue'] == result['Total_Revenue'].max()]

# Select only relevant columns and sort according to the supplier key
final_result = max_revenue_supplier[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'Total_Revenue']]\
    .sort_values('S_SUPPKEY')

# Write the results to a CSV file
final_result.to_csv('query_output.csv', index=False)
