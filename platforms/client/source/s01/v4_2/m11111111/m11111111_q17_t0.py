from pymongo import MongoClient
import csv
import pandas as pd

# create a client to connect to mongodb server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# create pandas dataframes with the part and lineitem collections
part_df = pd.DataFrame(list(db['part'].find({},{'_id':0})))
lineitem_df = pd.DataFrame(list(db['lineitem'].find({},{'_id':0})))

# merge the tables using common key partkey
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# filter based on brand and container
filtered_df = merged_df[(merged_df['P_BRAND'] == 'Brand#23') & (merged_df['P_CONTAINER'] == 'MED BAG')]

# compute average quantity for each partkey in lineitem
avg_qty = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_qty['AVG_QUANTITY'] = avg_qty['L_QUANTITY'] * 0.2
avg_qty = avg_qty.drop('L_QUANTITY', axis=1)

# join with the filtered data
joined_df = pd.merge(filtered_df, avg_qty, how='inner', left_on='L_PARTKEY', right_on='L_PARTKEY')

# further filter where L_QUANTITY < AVG_QUANTITY
final_df = joined_df[joined_df['L_QUANTITY'] < joined_df['AVG_QUANTITY']]

# compute the final result
result = final_df['L_EXTENDEDPRICE'].sum() / 7.0

# write the result to csv
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['AVG_YEARLY'])
    writer.writerow([result])
