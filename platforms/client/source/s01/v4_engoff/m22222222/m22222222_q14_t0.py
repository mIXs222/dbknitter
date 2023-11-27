from direct_redis import DirectRedis
import pandas as pd

# Connect to Redis instance
redis = DirectRedis(host='redis', port=6379, db=0)
# Get data from Redis
part_df = pd.read_json(redis.get('part'), orient='records')
lineitem_df = pd.read_json(redis.get('lineitem'), orient='records')

# Process data according to the query
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1995-09-01') & (lineitem_df['L_SHIPDATE'] <= '1995-10-01')
]
merged_df = pd.merge(filtered_lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Check for promotional parts
# As there's no direct indication of what constitutes a promotional part in the provided schema,
# I'll assume parts with a special flag in P_COMMENT or P_NAME indicate a promotion.
# The below line is a placeholder and should be replaced with the actual logic for identifying promotional parts.
merged_df['PROMOTIONAL'] = merged_df['P_COMMENT'].str.contains('promo') | merged_df['P_NAME'].str.contains('promo')

# Calculate total revenue and promotional revenue
total_revenue = merged_df['REVENUE'].sum()
promotional_revenue = merged_df[merged_df['PROMOTIONAL']]['REVENUE'].sum()
percentage_promotional_revenue = (promotional_revenue / total_revenue) * 100 if total_revenue > 0 else 0

# Output result
result = pd.DataFrame({
    'Total Revenue': [total_revenue],
    'Promotional Revenue': [promotional_revenue],
    'Percentage Promotional Revenue': [percentage_promotional_revenue]
})

result.to_csv('query_output.csv', index=False)
