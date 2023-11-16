import redis
import pandas as pd
from pandas import DataFrame
pd.options.mode.chained_assignment = None

# Connect to Redis
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# Read data from Redis
lineitem_data = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Filter dates
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
date_filter = lineitem_df['L_SHIPDATE'] <= '1998-09-02'
filtered_df = lineitem_df[date_filter]

# Group and aggregate data
grouped_df = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    SUM_QTY=('L_QUANTITY', 'sum'),
    SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
    SUM_DISC_PRICE=('L_EXTENDEDPRICE', lambda x: sum(x * (1 - filtered_df['L_DISCOUNT']))),
    SUM_CHARGE=('L_EXTENDEDPRICE', lambda x: sum(x * (1 - filtered_df['L_DISCOUNT']) * (1 + filtered_df['L_TAX']))),
    AVG_QTY=('L_QUANTITY', 'mean'),
    AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
    AVG_DISC=('L_DISCOUNT', 'mean'),
    COUNT_ORDER=('L_ORDERKEY', lambda x: len(x)),
).reset_index()

# Order data
ordered_df = grouped_df.sort_values(['L_RETURNFLAG', 'L_LINESTATUS'])

# Write data to file
ordered_df.to_csv('query_output.csv', index=False)
