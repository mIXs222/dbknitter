from pymongo import MongoClient
import pandas as pd

# Establishing Connection
client = MongoClient("mongodb://mongodb:27017/")
db = client['tpch']

# Fetching the necessary data from orders and lineitem collections
orders_data = pd.DataFrame(list(db.orders.find({},{"O_ORDERKEY":1, "O_ORDERPRIORITY":1})))
lineitem_data = pd.DataFrame(list(db.lineitem.find({},{"L_ORDERKEY":1, "L_SHIPMODE":1, "L_COMMITDATE":1, "L_RECEIPTDATE":1, "L_SHIPDATE":1})))

# Join the datasets on key
df = pd.merge(orders_data, lineitem_data, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Perform the filtering as per the WHERE clause
df = df[(df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) & (df['L_COMMITDATE'] < df['L_RECEIPTDATE']) & (df['L_SHIPDATE'] < df['L_COMMITDATE']) & (df['L_RECEIPTDATE'] >= '1994-01-01') & (df['L_RECEIPTDATE'] < '1995-01-01')]

# Create new columns for HIGH_LINE_COUNT and LOW_LINE_COUNT
df['HIGH_LINE_COUNT'] = df['O_ORDERPRIORITY'].apply(lambda x: 1 if x in ['1-URGENT', '2-HIGH'] else 0)
df['LOW_LINE_COUNT'] = df['O_ORDERPRIORITY'].apply(lambda x: 1 if x not in ['1-URGENT', '2-HIGH'] else 0)

# Perform groupby operation
grouped_df = df.groupby('L_SHIPMODE').sum()[['HIGH_LINE_COUNT', 'LOW_LINE_COUNT']].reset_index()

# Sort the DataFrame
grouped_df = grouped_df.sort_values(by=['L_SHIPMODE'])

# Write CSV file
grouped_df.to_csv('query_output.csv', index=False)
