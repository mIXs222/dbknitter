import pandas as pd
import pymongo
from pymongo import MongoClient
import csv

# MongoDB connection
client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

part_df = pd.DataFrame(list(db['part'].find()))
lineitem_df = pd.DataFrame(list(db['lineitem'].find()))

# Joining tables
joined_df = pd.merge(lineitem_df, part_df, left_on="L_PARTKEY", right_on="P_PARTKEY")

# Applying conditions
joined_df = joined_df[
    (joined_df['L_SHIPDATE'] >= '1995-09-01') &
    (joined_df['L_SHIPDATE'] < '1995-10-01')
]

# Calculating revenue
joined_df['REVENUE'] = joined_df.apply(
    lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) if row['P_TYPE'].startswith('PROMO') 
    else 0, axis=1
)

# Calculating promo_revenue
promo_revenue = (100.00 * joined_df['REVENUE'].sum()) / (joined_df['L_EXTENDEDPRICE'] * (1 - joined_df['L_DISCOUNT'])).sum()

# Writing to csv file
with open('query_output.csv', 'w', newline='') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(['PROMO_REVENUE'])
    wr.writerow([promo_revenue])
