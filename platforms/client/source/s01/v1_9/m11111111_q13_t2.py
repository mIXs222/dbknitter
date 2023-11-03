from pymongo import MongoClient
import pandas as pd
from pandasql import sqldf

# This establishes connection with MongoDB client
myclient = MongoClient("mongodb://mongodb:27017/")

# This will select the database
mydb = myclient["tpch"]

# This will select the table
customer_col = mydb["customer"]
orders_col = mydb["orders"]

# This will get the data
customer_data = pd.DataFrame(list(customer_col.find({},{'_id':0})))
orders_data = pd.DataFrame(list(orders_col.find({},{'_id':0})))

# This will merge both the dataframes
merged_df = pd.merge(customer_data, orders_data, left_on = 'C_CUSTKEY', right_on = 'O_CUSTKEY', how = 'left')

# This will filter the data
merged_df = merged_df[~merged_df.O_COMMENT.str.contains('pending deposits', na = False)]

# This applies Group By on the dataframe
grouped_df = merged_df.groupby(['C_CUSTKEY'], as_index = False).agg({'O_ORDERKEY' : 'count'}).rename(columns = {'O_ORDERKEY' : 'C_COUNT'})

# This runs the SQL Query
sqldf_run = sqldf("SELECT C_COUNT, COUNT(*) AS CUSTDIST FROM grouped_df GROUP BY C_COUNT ORDER BY CUSTDIST DESC, C_COUNT DESC")

# This writes to csv
sqldf_run.to_csv('query_output.csv', index = False)
