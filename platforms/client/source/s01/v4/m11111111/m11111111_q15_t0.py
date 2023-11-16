# query_code.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# Connect to the MongoDB server
client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# Read the collections into DataFrames
suppliers_df = pd.DataFrame(list(db.supplier.find()))
lineitem_df = pd.DataFrame(list(db.lineitem.find()))

# Convert L_SHIPDATE from string to datetime for filtering
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter `lineitem` for L_SHIPDATE and calculate revenue for each L_SUPPKEY
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)

revenue0_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) &
    (lineitem_df['L_SHIPDATE'] < end_date)
].groupby('L_SUPPKEY').apply(
    lambda df: pd.Series({
        "TOTAL_REVENUE": (df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])).sum()
    })
).reset_index()

# Find the supplier with the maximum revenue
max_revenue = revenue0_df['TOTAL_REVENUE'].max()
top_supplier_df = revenue0_df[revenue0_df['TOTAL_REVENUE'] == max_revenue]

# Merge supplier and revenue data on S_SUPPKEY
result_df = pd.merge(
    suppliers_df,
    top_supplier_df,
    left_on='S_SUPPKEY',
    right_on='L_SUPPKEY'
)

# Select the desired columns and order by S_SUPPKEY
final_result_df = result_df[[
    'S_SUPPKEY',
    'S_NAME',
    'S_ADDRESS',
    'S_PHONE',
    'TOTAL_REVENUE'
]].sort_values('S_SUPPKEY')

# Write the result to a CSV
final_result_df.to_csv('query_output.csv', index=False)
