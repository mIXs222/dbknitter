import pymongo
from pymongo import MongoClient
import mysql.connector
import pandas as pd

# connect to mongodb
client = MongoClient('mongodb://mongodb:27017/')
db = client.tpch

# load mongodb tables into pandas dataframes
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# merging the mongo db dataframes
merged_df = pd.merge(orders, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# filtering the merged dataframe
filtered_df = merged_df[(merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) & (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) & (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) & (merged_df['L_RECEIPTDATE'] >= '1994-01-01') & (merged_df['L_RECEIPTDATE'] < '1995-01-01')]

# apply group by and further operations on the filtered dataframe
result_df = filtered_df.groupby('L_SHIPMODE').agg({'O_ORDERPRIORITY': [('HIGH_LINE_COUNT','apply', lambda x: sum((x == '1-URGENT') | (x == '2-HIGH'))), ('LOW_LINE_COUNT','apply', lambda x: sum((x != '1-URGENT') & (x != '2-HIGH')))]})

# write the output to csv file
result_df.to_csv('query_output.csv')
