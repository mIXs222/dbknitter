# Your Python script must be able to interact with MongoDB 
from pymongo import MongoClient
import pandas as pd
from datetime import datetime 

# Connect to your MongoDB cluster
client = MongoClient("mongodb://mongodb:27017/")

# Select your database
db = client["tpch"]

# Select the collection (table)
lineitem = db["lineitem"]

# Build the criteria for the query
start_date = datetime.strptime('1994-01-01', "%Y-%m-%d")
end_date = datetime.strptime('1995-01-01', "%Y-%m-%d")
criteria = {
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date},
    'L_DISCOUNT': {'$gte': .06 - 0.01, '$lte': .06 + 0.01},
    'L_QUANTITY': {'$lt': 24}
}

# Execute the query
results = lineitem.find(criteria)

# Create pandas dataframe from the result
df = pd.DataFrame(list(results))

# Calculate REVENUE
df['REVENUE'] = df['L_EXTENDEDPRICE'] * df['L_DISCOUNT']
total_revenue = df['REVENUE'].sum()

# Write the output to a CSV file
with open('query_output.csv', 'w') as file:
    file.write(f'REVENUE\n{total_revenue}\n')
