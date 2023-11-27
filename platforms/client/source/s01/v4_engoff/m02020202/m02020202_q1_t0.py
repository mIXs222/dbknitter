import datetime
import pandas as pd
from direct_redis import DirectRedis

# Redis connection information
hostname = 'redis'
port = 6379
database_name = 0

# Connect to Redis
r = DirectRedis(host=hostname, port=port, db=database_name)

# Retrieve and convert to Pandas DataFrame
lineitem_df = pd.read_msgpack(r.get('lineitem'))

# Convert dates to datetime and filter records shipped before 1998-09-02
date_cutoff = datetime.datetime(1998, 9, 2)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] < date_cutoff]

# Calculate group aggregates
grouped = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS'])
summary = grouped['L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'].agg({
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': 'mean',
    'L_EXTENDEDPRICE': lambda x: sum(x * (1 - filtered_df['L_DISCOUNT'])),
    'L_EXTENDEDPRICE': lambda x: sum(x * (1 - filtered_df['L_DISCOUNT']) * (1 + filtered_df['L_TAX'])),
}).reset_index()

summary.columns = ['L_RETURNFLAG', 'L_LINESTATUS', 
                   'SUM_QUANTITY', 'AVG_QUANTITY', 
                   'SUM_EXTENDEDPRICE', 'AVG_EXTENDEDPRICE', 
                   'AVG_DISCOUNT', 'SUM_DISC_PRICE', 'SUM_DISC_PRICE_PLUS_TAX']

# Add count of lineitems
summary['COUNT_ORDER'] = grouped.size().values

# Sort by L_RETURNFLAG and L_LINESTATUS
sorted_summary = summary.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Write to CSV file
sorted_summary.to_csv('query_output.csv', index=False)
