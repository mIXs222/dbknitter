from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# Create a connection to the MongoDB
client = MongoClient('mongodb://mongodb:27017/')

# Select the database
db = client['tpch']

# Select the collections (analogue of tables in SQL)
lineitem = db['lineitem']
part = db['part']

# Dates for WHERE clause
date_from = datetime.strptime('1995-09-01', '%Y-%m-%d')
date_to = datetime.strptime('1995-10-01', '%Y-%m-%d')

# Perform the MongoDB equivalent of SQL JOIN
pipeline = [
    {
        '$lookup': {
            'from': 'part',
            'localField': 'L_PARTKEY',
            'foreignField': 'P_PARTKEY',
            'as': 'part_info'
        }
    },
    {
        '$match': {
            'L_SHIPDATE': {'$gte': date_from, '$lt': date_to}
        }
    }
]

data = list(lineitem.aggregate(pipeline))

# Convert the result to Pandas DataFrame
df = pd.json_normalize(data)

# Apply the calculation
df['L_EXTENDEDPRICE'] = df['L_EXTENDEDPRICE'].astype(float)
df['L_DISCOUNT'] = df['L_DISCOUNT'].astype(float)
df['REVENUE'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
df['PROMO_REVENUE'] = df['REVENUE'].where(df['part_info.P_TYPE'].str.startswith('PROMO'), 0)

# Calculate the result of the query
result = 100.00 * df['PROMO_REVENUE'].sum() / df['REVENUE'].sum()

# Create output DataFrame
output = pd.DataFrame([result], columns=['PROMO_REVENUE'])

# Write to CSV
output.to_csv('query_output.csv', index=False)
