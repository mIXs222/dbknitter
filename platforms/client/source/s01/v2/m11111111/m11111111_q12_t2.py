import pandas as pd
from pymongo import MongoClient
from pandas.io.json import json_normalize
import csv

# establish a connection to the MongoDB:
client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# load the documents in collections:
orders = json_normalize(list(db.orders.find()))
lineitem = json_normalize(list(db.lineitem.find()))

# Merge the two collections based on matching O_ORDERKEY and L_ORDERKEY
merged_collections = pd.merge(orders, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# apply the conditions:
filtered_collections = merged_collections[
    (merged_collections.L_SHIPMODE.isin(['MAIL', 'SHIP'])) &
    (pd.to_datetime(merged_collections.L_COMMITDATE) < pd.to_datetime(merged_collections.L_RECEIPTDATE)) &
    (pd.to_datetime(merged_collections.L_SHIPDATE) < pd.to_datetime(merged_collections.L_COMMITDATE)) &
    (pd.to_datetime(merged_collections.L_RECEIPTDATE) >= pd.Timestamp('1994-01-01')) &
    (pd.to_datetime(merged_collections.L_RECEIPTDATE) < pd.Timestamp('1995-01-01'))
]

filtered_collections["HIGH_LINE_COUNT"] = (filtered_collections.O_ORDERPRIORITY.isin(['1-URGENT', '2-HIGH'])).astype(int)
filtered_collections["LOW_LINE_COUNT"] = (~filtered_collections.O_ORDERPRIORITY.isin(['1-URGENT', '2-HIGH'])).astype(int)

output = filtered_collections.groupby('L_SHIPMODE')[['HIGH_LINE_COUNT', 'LOW_LINE_COUNT']].sum().reset_index()

output.sort_values('L_SHIPMODE', inplace=True)

# Write the resulting DataFrame to a CSV file
output.to_csv('query_output.csv', index=False)
