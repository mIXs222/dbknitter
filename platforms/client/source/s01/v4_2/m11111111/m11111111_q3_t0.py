# Required Libraries
import pandas as pd
from pymongo import MongoClient
import datetime

# Connecting to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Loading tables from MongoDB
customer_df = pd.DataFrame(list(db['customer'].find()))
orders_df = pd.DataFrame(list(db['orders'].find()))
lineitem_df = pd.DataFrame(list(db['lineitem'].find()))

# Converting dates from string to datetime format
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'], errors='coerce')
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'], errors='coerce')

# Joining all tables together
df = customer_df.merge(orders_df, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = df.merge(lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filtering data
filtered_df = df[df['C_MKTSEGMENT']=='BUILDING']
filtered_df = filtered_df[(filtered_df['O_ORDERDATE'] < datetime.datetime(1995, 3, 15)) &
                          (filtered_df['L_SHIPDATE'] > datetime.datetime(1995, 3, 15))]

# Calculating revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Grouping data
grouped_df = filtered_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({'REVENUE': 'sum'}).reset_index()

# Sorting data
sorted_df = grouped_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Writing data to CSV
sorted_df.to_csv('query_output.csv', index=False)
