from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb', 27017)
db = client['tpch']
orders = db['orders']
lineitem = db['lineitem']

# Fetch orders data
orders_data = orders.find({
    "O_ORDERDATE": {
        "$gte": "1993-07-01",
        "$lt": "1993-10-01"
    }
})

# Convert orders data to dataframe
orders_df = pd.DataFrame(list(orders_data))

# Fetch lineitem data
lineitem_data = lineitem.find()

# Convert lineitem data to dataframe
lineitem_df = pd.DataFrame(list(lineitem_data))

# Merge orders and lineitem data
combined_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter data where L_COMMITDATE < L_RECEIPTDATE
filtered_df = combined_df[combined_df['L_COMMITDATE'] < combined_df['L_RECEIPTDATE']]

# Group and count data
result_df = filtered_df.groupby('O_ORDERPRIORITY')['O_ORDERPRIORITY'].count().reset_index(name='ORDER_COUNT')

# Sort data
result_df.sort_values(by=['O_ORDERPRIORITY'], inplace=True)

# Write data to CSV
result_df.to_csv('query_output.csv', index=False)
