from pymongo import MongoClient
import pandas as pd
from bson.decimal128 import Decimal128

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch

# Step 1: Query customers with segment 'BUILDING'
customers = list(db.customer.find({'C_MKTSEGMENT': 'BUILDING'}, {'C_CUSTKEY': 1}))

# Convert the list of dictionaries to a DataFrame
df_customers = pd.DataFrame(customers).rename(columns={"C_CUSTKEY": "O_CUSTKEY"})

# Step 2: Join customers with orders on `C_CUSTKEY` = `O_CUSTKEY`
orders = list(db.orders.find({'O_ORDERDATE': {'$lt': '1995-03-15'}}))
df_orders = pd.DataFrame(orders).rename(columns={"O_ORDERKEY": "L_ORDERKEY", "O_CUSTKEY": "O_CUSTKEY"})

# Step 3: Join orders with lineitem on `O_ORDERKEY` = `L_ORDERKEY`
lineitem = list(db.lineitem.find({'L_SHIPDATE': {'$gt': '1995-03-15'}}))
df_lineitem = pd.DataFrame(lineitem)

# Merge DataFrames
merged_df = df_customers.merge(df_orders, on="O_CUSTKEY").merge(df_lineitem, on="L_ORDERKEY")
filtered_df = merged_df[(merged_df['O_ORDERDATE'] < '1995-03-15') & (merged_df['L_SHIPDATE'] > '1995-03-15')]

# Calculate revenue
filtered_df['REVENUE'] = filtered_df.apply(lambda x: (x['L_EXTENDEDPRICE'].to_decimal() * (1 - x['L_DISCOUNT'].to_decimal())).to_decimal(), axis=1)

# Group by
grouped_df = filtered_df.groupby(by=['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({
    'REVENUE': 'sum'
}).reset_index()

# Sort the result
final_df = grouped_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write the output to CSV
final_df.to_csv('query_output.csv', index=False)
