import pandas as pd
import direct_redis

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get DataFrames from Redis
supplier_df = pd.DataFrame(redis_client.get('supplier'))
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Convert relevant columns to datetime for lineitem DataFrame
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Create revenue0 DataFrame with conditions and aggregation
revenue0 = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1996-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1996-04-01')
].groupby('L_SUPPKEY').agg(
    TOTAL_REVENUE=pd.NamedAgg(column='L_EXTENDEDPRICE',
                              aggfunc=lambda x: sum(x * (1 - lineitem_df.loc[x.index, 'L_DISCOUNT'])))
).reset_index().rename(columns={'L_SUPPKEY': 'SUPPLIER_NO'})

# Merge supplier DataFrame with revenue0 DataFrame
result = supplier_df.merge(revenue0, how='inner', left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

# Find the max TOTAL_REVENUE 
max_total_revenue = revenue0['TOTAL_REVENUE'].max()

# Filter the result where TOTAL_REVENUE is max
final_result = result[result['TOTAL_REVENUE'] == max_total_revenue]

# Sort the DataFrame by S_SUPPKEY
final_result.sort_values(by='S_SUPPKEY', inplace=True)

# Select the required fields
final_result = final_result[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Write the result to CSV
final_result.to_csv('query_output.csv', index=False)
