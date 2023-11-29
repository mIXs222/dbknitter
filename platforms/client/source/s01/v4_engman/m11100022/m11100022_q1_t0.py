import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to Redis
redis_host = "redis"
redis_port = 6379
redis_db = 0

redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Retrieve data from Redis
lineitem_df = redis_client.get('lineitem')

# Convert the ship date column to datetime
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Apply the date filter before generating the report
date_filter = lineitem_df['L_SHIPDATE'] < "1998-09-02"
filtered_df = lineitem_df[date_filter]

# Calculate aggregates
report_df = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    TOTAL_QUANTITY=('L_QUANTITY', 'sum'),
    TOTAL_EXTENDEDPRICE=('L_EXTENDEDPRICE', 'sum'),
    TOTAL_DISCOUNTPRICE=lambda x: (x['L_QUANTITY'] * x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum(),
    TOTAL_DISCOUNTED_PRICE_PLUS_TAX=lambda x: (x['L_QUANTITY'] * x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) * (1 + x['L_TAX'])).sum(),
    AVG_QUANTITY=('L_QUANTITY', 'mean'),
    AVG_EXTENDED_PRICE=('L_EXTENDEDPRICE', 'mean'),
    AVG_DISCOUNT=('L_DISCOUNT', 'mean'),
    COUNT_ORDER=('L_ORDERKEY', 'count')
).reset_index()

# Sort the result
sorted_report_df = report_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Write the result to a CSV file
sorted_report_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
