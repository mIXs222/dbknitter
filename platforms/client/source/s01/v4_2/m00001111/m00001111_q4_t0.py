# import necessary libraries
from pymongo import MongoClient
import pandas as pd
import datetime

# setup connection
client = MongoClient("mongodb://mongodb:27017")
db = client["tpch"]

# converting SQL date string to Python date object
start_date = datetime.datetime.strptime('1993-07-01', '%Y-%m-%d')
end_date = datetime.datetime.strptime('1993-10-01', '%Y-%m-%d')

# filter records based on date >= start_date, date < end_date and existence of orderkey in lineitem
orders = [order for order in db.orders.find({"O_ORDERDATE": {"$gte": start_date, "$lt": end_date}}) 
           if db.lineitem.find_one({"L_ORDERKEY": order["O_ORDERKEY"], "L_COMMITDATE": {"$lt": order["O_ORDERDATE"]}})]

# convert data to pandas dataframe
df = pd.DataFrame(orders)

# create group by O_ORDERPRIORITY and write the result into a CSV file.
output = df.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT').sort_values(by='O_ORDERPRIORITY')
output.to_csv('query_output.csv', index=False)
