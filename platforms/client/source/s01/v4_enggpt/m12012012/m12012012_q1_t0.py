import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to Redis
hostname = "redis"
port = 6379
database_name = 0

# Create a DirectRedis client connection
redis_client = DirectRedis(host=hostname, port=port, db=database_name)

# Fetch the data from Redis
lineitem_data = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Convert string dates to datetime format for comparison (if stored as strings)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter the DataFrame based on shipping date condition
filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] <= '1998-09-02']

# Perform aggregations according to the query
grouped = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    SUM_QTY=('L_QUANTITY', 'sum'),
    SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
    SUM_DISC_PRICE=(lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()),
    SUM_CHARGE=(lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) * (1 + x['L_TAX'])).sum()),
    AVG_QTY=('L_QUANTITY', 'mean'),
    AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
    AVG_DISC=('L_DISCOUNT', 'mean'),
    COUNT_ORDER=('L_ORDERKEY', 'nunique')
).reset_index()

# Sort the results as required
grouped_sorted = grouped.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Save the results to CSV
grouped_sorted.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
