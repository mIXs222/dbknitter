# analysis.py
import pandas as pd
import direct_redis

# Connect to the redis DB
hostname = 'redis'
port = 6379
db_number = 0

# Since regular redis.Redis is replaced by direct_redis.DirectRedis as per instruction
r = direct_redis.DirectRedis(host=hostname, port=port, db=db_number)

# Read the lineitem table from Redis into a Pandas DataFrame
lineitem_df = r.get('lineitem')

# Convert strings to datetime where necessary and filter the DataFrame
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] <= pd.Timestamp('1998-09-02')]

# Group the filtered DataFrame by L_RETURNFLAG and L_LINESTATUS and perform the aggregate operations
result_df = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg({
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': ['mean', 'sum'],
    'L_ORDERKEY': 'count',
    'L_TAX': 'sum'
}).reset_index()

# Rename the columns as per the requirement
result_df.columns = [
    'L_RETURNFLAG',
    'L_LINESTATUS',
    'SUM_QTY',
    'AVG_QTY',
    'SUM_BASE_PRICE',
    'AVG_PRICE',
    'AVG_DISC',
    'SUM_DISC',
    'COUNT_ORDER',
    'SUM_TAX'
]

# Calculate SUM_DISC_PRICE and SUM_CHARGE
result_df['SUM_DISC_PRICE'] = result_df['SUM_BASE_PRICE'] * (1 - result_df['AVG_DISC'])
result_df['SUM_CHARGE'] = result_df['SUM_DISC_PRICE'] * (1 + result_df['SUM_TAX'])

# Reorder columns to match the specified output
result_df = result_df[['L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER']]

# Sort by return flag and line status
result_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
