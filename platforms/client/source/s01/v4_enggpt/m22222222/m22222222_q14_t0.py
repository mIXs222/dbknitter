import direct_redis
import pandas as pd
import numpy as np

# Establish a connection to the Redis database
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read DataFrames from Redis
df_part = pd.DataFrame(eval(r.get('part')))
df_lineitem = pd.DataFrame(eval(r.get('lineitem')))

# Convert L_SHIPDATE from string to datetime and filter the data by the specified timeframe
df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])
filtered_lineitem = df_lineitem[(df_lineitem['L_SHIPDATE'] >= pd.Timestamp('1995-09-01')) &
                                (df_lineitem['L_SHIPDATE'] <= pd.Timestamp('1995-09-30'))]

# Merge the two DataFrames on partkey with the relevant promotion filter for parts
promo_parts = df_part[df_part['P_TYPE'].str.startswith('PROMO')]
merged_df = pd.merge(filtered_lineitem, promo_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the sum of extended price for the promotional items adjusted by discount
promo_revenue = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])).sum()

# Calculate the total sum of extended prices for all items adjusted by discount
total_revenue = (filtered_lineitem['L_EXTENDEDPRICE'] * (1 - filtered_lineitem['L_DISCOUNT'])).sum()

# Calculate the promotional revenue percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Output results
result = pd.DataFrame([{'Promotional Revenue (%)': promo_revenue_percentage}])
result.to_csv('query_output.csv', index=False)
