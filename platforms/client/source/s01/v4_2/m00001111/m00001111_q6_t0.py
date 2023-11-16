from pymongo import MongoClient
import pandas as pd

# Setting up the connection to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client['tpch']
collection = db['lineitem']

# Parsing the date strings to datetime objects
from datetime import datetime
date_format = "%Y-%m-%d"

start_date = datetime.strptime('1994-01-01', date_format)
end_date = datetime.strptime('1995-01-01', date_format)

# Constructing the query
query = {'$and': [{'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}}, 
                  {'L_DISCOUNT':{'$gte': .06 - 0.01, '$lte': .06 + 0.01}}, 
                  {'L_QUANTITY':{'$lt': 24}}]}

result = collection.find(query,{'_id':0,'L_EXTENDEDPRICE':1, 'L_DISCOUNT':1})

# Calculate the Revenue:
revenue = sum([record['L_EXTENDEDPRICE']* record['L_DISCOUNT'] for record in result])

# Create the dataframe
df = pd.DataFrame({'REVENUE': [revenue]})

# Save the revenue value into a csv
df.to_csv('query_output.csv', index=False)

print("Revenue was calculated and saved to 'query_output.csv'.")
