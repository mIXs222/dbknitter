import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Function to get DataFrame from Redis
def get_df_from_redis(table_name):
    return pd.DataFrame(redis_client.get(table_name))

# Retrieve data
part_df = get_df_from_redis('part')
lineitem_df = get_df_from_redis('lineitem')

# Define conditions for the analysis
conditions = [
    (part_df['P_BRAND'] == 'Brand#12') &
    (part_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
    (lineitem_df['L_QUANTITY'] >= 1) & (lineitem_df['L_QUANTITY'] <= 11) &
    (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 5) &
    (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),

    (part_df['P_BRAND'] == 'Brand#23') &
    (part_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
    (lineitem_df['L_QUANTITY'] >= 10) & (lineitem_df['L_QUANTITY'] <= 20) &
    (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 10) &
    (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'),

    (part_df['P_BRAND'] == 'Brand#34') &
    (part_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
    (lineitem_df['L_QUANTITY'] >= 20) & (lineitem_df['L_QUANTITY'] <= 30) &
    (part_df['P_SIZE'] >= 1) & (part_df['P_SIZE'] <= 15) &
    (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
]

# Combining conditions with an OR clause and calculating total revenue
combined_condition = conditions[0]
for cond in conditions[1:]:
    combined_condition |= cond

# Filtering the data sets based on the conditions
filtered_lines = lineitem_df[combined_condition]

# Calculating revenue
filtered_lines['REVENUE'] = filtered_lines['L_EXTENDEDPRICE'] * (1 - filtered_lines['L_DISCOUNT'])

# Summing up revenue
total_revenue = filtered_lines['REVENUE'].sum()

# Output results to a CSV file
output_df = pd.DataFrame([{'Total Revenue': total_revenue}])
output_df.to_csv('query_output.csv', index=False)
