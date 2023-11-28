import pandas as pd
from direct_redis import DirectRedis
import csv

# Establish a connection to the Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Read the data frames using the keys 'part' and 'lineitem'
part_df = pd.read_json(redis_client.get('part'), orient='records')
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Filter the parts based on brand and container type
filtered_parts = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Calculate the average quantity for each part key
avg_quantity = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()

# Merge the average quantity with the lineitem table
lineitem_with_avg_qty = lineitem_df.merge(avg_quantity, how='left', left_on='L_PARTKEY', right_on='L_PARTKEY')

# Filter the line items
filtered_lineitems = lineitem_with_avg_qty[
    (lineitem_with_avg_qty['L_QUANTITY_x'] < 0.2 * lineitem_with_avg_qty['L_QUANTITY_y']) &
    (lineitem_with_avg_qty['L_PARTKEY'].isin(filtered_parts['P_PARTKEY']))
]

# Calculate the average yearly extended price
filtered_lineitems['YEARLY_EXTENDED_PRICE'] = filtered_lineitems['L_EXTENDEDPRICE'] / 7.0
avg_yearly_extended_price = filtered_lineitems.groupby('L_PARTKEY')['YEARLY_EXTENDED_PRICE'].mean().reset_index()

# Write the output to a CSV file
avg_yearly_extended_price.to_csv('query_output.csv', index=False)
