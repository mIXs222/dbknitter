import pymongo
import pandas as pd
from datetime import datetime

# Establish connection to MongoDB instance
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']

# Load 'part' and 'lineitem' collections into Pandas DataFrames
part_col = db['part']
lineitem_col = db['lineitem']

# Convert collections to DataFrames
part_df = pd.DataFrame(list(part_col.find({'P_BRAND': 'BRAND#23', 'P_CONTAINER': 'MED BAG'})))
lineitem_df = pd.DataFrame(list(lineitem_col.find()))

# Merge part and lineitem DataFrames on P_PARTKEY and L_PARTKEY
merged_df = lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter for lineitems with matched parts
filtered_lineitems = merged_df[(merged_df['P_BRAND'] == 'BRAND#23') & (merged_df['P_CONTAINER'] == 'MED BAG')]

# Calculate the average quantity of such lineitems
avg_quantity = filtered_lineitems['L_QUANTITY'].mean()

# Calculate lineitem average yearly gross loss
filtered_lineitems['yearly_loss'] = filtered_lineitems['L_EXTENDEDPRICE'] * (filtered_lineitems['L_QUANTITY'] < (0.2 * avg_quantity))
filtered_lineitems['L_SHIPDATE_year'] = filtered_lineitems['L_SHIPDATE'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').year)

# Calculate the average yearly loss and write it to a CSV
result = filtered_lineitems.groupby('L_SHIPDATE_year')['yearly_loss'].sum().reset_index()

# Assuming we have 7 years of data to take the average over
result['avg_yearly_loss'] = result['yearly_loss'] / 7

# Outputting the result to CSV
result[['L_SHIPDATE_year', 'avg_yearly_loss']].to_csv('query_output.csv', index=False)

# Close MongoDB connection
client.close()
