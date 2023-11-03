import pymongo
from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize

# Connect to MongoDB
client = MongoClient("mongodb://mongodb:27017/")
db = client['tpch']

# MongoDB queries
orders = db.orders.find({})
lineitem = db.lineitem.find({})

# Normalize JSON to Dataframe
df_orders = json_normalize(list(orders))
df_lineitem = json_normalize(list(lineitem))

# Merge Dataframes on Key
merged_df = pd.merge(df_orders, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

# Apply Filters
filtered_df = merged_df[(merged_df['L_SHIPMODE'].isin(['MAIL','SHIP'])) 
                        & (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) 
                        & (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) 
                        & (merged_df['L_RECEIPTDATE'] >= '1994-01-01') 
                        & (merged_df['L_RECEIPTDATE'] < '1995-01-01')]

# Define function for priority cases
def priority_cases(row):
    if row['O_ORDERPRIORITY'] == '1-URGENT' or row['O_ORDERPRIORITY'] == '2-HIGH':
        return 'HIGH_LINE_COUNT'
    else:
        return 'LOW_LINE_COUNT'

# Apply function to Dataframe
filtered_df['PRIORITY'] = filtered_df.apply(priority_cases, axis=1)

# Group by shipmode and priority then reset index
grouped_df = filtered_df.groupby(['L_SHIPMODE', 'PRIORITY']).size().reset_index(name='COUNTS')

# Pivot the Dataframe to the final form and fill NaN with 0
pivot_df = grouped_df.pivot_table(index='L_SHIPMODE', columns='PRIORITY', values='COUNTS').fillna(0)

# Write to CSV
pivot_df.to_csv('query_output.csv')
