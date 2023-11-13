from pymongo import MongoClient
import pandas as pd
from datetime import datetime

client = MongoClient('mongodb://localhost:27017/')  # create a MongoClient to the running mongod instance
db = client['tpch']  # access the tpch database
lineitem_collection = db['lineitem']  # access the lineitem table

# Retrieve all documents from the lineitem collection
lineitem_data = list(lineitem_collection.find())

# Convert list of dictionaries to DataFrame
data_df = pd.DataFrame(lineitem_data)

# Convert string to datetime
data_df['L_SHIPDATE'] = pd.to_datetime(data_df['L_SHIPDATE'])

# Apply filtering conditions
filtered_data_df = data_df[
    (data_df['L_SHIPDATE'] >= datetime(1994, 1, 1)) &
    (data_df['L_SHIPDATE'] < datetime(1995, 1, 1)) &
    (data_df['L_DISCOUNT'].between(0.06 - 0.01, 0.06 + 0.01)) &   
    (data_df['L_QUANTITY'] < 24)
]

# Calculate the sum 
revenue = filtered_data_df['L_DISCOUNT'].multiply(filtered_data_df['L_EXTENDEDPRICE']).sum()

# Write result to a CSV file
with open('query_output.csv', 'w') as f:
    f.write("REVENUE\n")
    f.write(str(revenue))
