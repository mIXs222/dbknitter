# pricing_summary_report.py
import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
hostname = 'redis'
port = 6379
database_name = '0'
redis_client = DirectRedis(host=hostname, port=port, db=database_name)

# Read the 'lineitem' table from Redis
lineitem_data = redis_client.get('lineitem')

# Create DataFrame from the fetched data
lineitem_df = pd.read_json(lineitem_data)

# Filter rows where L_SHIPDATE is before 1998-09-02
filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] < '1998-09-02']

# Calculate the required aggregates
aggregations = {
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': 'mean',
    'L_EXTENDEDPRICE_DISCOUNTED': [('sum', lambda x: (x * (1 - lineitem_df['L_DISCOUNT'])).sum()), ('mean', lambda x: (x * (1 - lineitem_df['L_DISCOUNT'])).mean())],
    'L_EXTENDEDPRICE_DISCOUNTED_TAX': [('sum', lambda x: (x * (1 - lineitem_df['L_DISCOUNT']) * (1 + lineitem_df['L_TAX'])).sum()), ('mean', lambda x: (x * (1 - lineitem_df['L_DISCOUNT']) * (1 + lineitem_df['L_TAX'])).mean())], 
    'count': pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')
}

# Apply the grouping
grouped_result = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(**aggregations).reset_index()

# Sort the results
sorted_result = grouped_result.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Rename the column to more readable format
sorted_result.columns = ['L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'AVG_QTY', 'SUM_BASE_PRICE', 'AVG_BASE_PRICE', 'AVG_DISCOUNT', 'SUM_DISC_PRICE', 'AVG_DISC_PRICE', 'SUM_CHARGE', 'AVG_CHARGE', 'COUNT_ORDER']

# Write the result to a CSV file
sorted_result.to_csv('query_output.csv', index=False)
