# file: execute_query.py
import pandas as pd
from direct_redis import DirectRedis

# Connection Information
hostname = 'redis'
port = 6379
database_name = '0'

# Connect to Redis
redis_client = DirectRedis(host=hostname, port=port, db=database_name)

# Retrieve DataFrames from Redis
df_supplier = pd.read_json(redis_client.get('supplier'))
df_lineitem = pd.read_json(redis_client.get('lineitem'))

# Preprocess date columns
df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])

# Perform the operations as specified in the SQL query
# Step 1: Filter lineitem data
filtered_lineitem = df_lineitem[
    (df_lineitem['L_SHIPDATE'] >= '1996-01-01') &
    (df_lineitem['L_SHIPDATE'] < '1996-04-01')
]

# Step 2: Calculate revenue0
revenue0 = filtered_lineitem.groupby('L_SUPPKEY').apply(
    lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()
).reset_index(name='TOTAL_REVENUE')

# Step 3: Rename columns
revenue0.rename(columns={'L_SUPPKEY': 'SUPPLIER_NO'}, inplace=True)

# Step 4: Join supplier with revenue0
result = pd.merge(
    df_supplier, revenue0,
    how='inner',
    left_on='S_SUPPKEY',
    right_on='SUPPLIER_NO'
)

# Step 5: Find max total revenue and filter accordingly
max_total_revenue = revenue0['TOTAL_REVENUE'].max()
result = result[result['TOTAL_REVENUE'] == max_total_revenue]

# Step 6: Select the required columns and sort by S_SUPPKEY
result = result[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']].sort_values('S_SUPPKEY')

# Step 7: Write output to CSV file
result.to_csv('query_output.csv', index=False)
