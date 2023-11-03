from pymongo import MongoClient
import pandas as pd

# connect to the MongoDB
client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# retrieve the data from MongoDB
lineitem = list(db['lineitem'].find({}, {"_id": 0}))
lineitem_df = pd.DataFrame(lineitem)

part = list(db['part'].find({}, {"_id": 0}))
part_df = pd.DataFrame(part)

# merge the data
df = pd.merge(lineitem_df, part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# filter the data
df = df[(df['P_BRAND'] == 'Brand#23') & (df['P_CONTAINER'] == 'MED BAG')]

# calculate the average L_QUANTITY
avg_quantity = df.groupby('P_PARTKEY')['L_QUANTITY'].mean() * 0.2

# filter rows where L_QUANTITY is less than 0.2 * average L_QUANTITY
df = df[df.apply(lambda row: row['L_QUANTITY'] < avg_quantity[row['P_PARTKEY']], axis=1)]

# calculate AVG_YEARLY
df['AVG_YEARLY'] = df['L_EXTENDEDPRICE'] / 7.0

# write the result to CSV
df.to_csv('query_output.csv', index=False)
