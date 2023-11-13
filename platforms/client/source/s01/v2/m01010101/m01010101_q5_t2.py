import pandas as pd
import pymysql
import pymongo
from pymongo import MongoClient
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='localhost', user='root', password='my-secret-pw', db='tpch')

# Execute the query on MySQL
mysql_query = "SELECT * FROM nation, orders"
mysql_data = pd.read_sql(mysql_query, mysql_conn)

mysql_conn.close()

# Connect to MongoDB
mongo_client = MongoClient('localhost', 27017)

# Get a reference to a particular database
db = mongo_client['tpch']

# Load all data from MongoDB collections into pandas dataframes
region_df = pd.DataFrame(list(db.region.find()))
supplier_df = pd.DataFrame(list(db.supplier.find()))
customer_df = pd.DataFrame(list(db.customer.find()))
lineitem_df = pd.DataFrame(list(db.lineitem.find()))

# Merge all dataframes
merged_data = pd.concat([mysql_data, region_df, supplier_df, customer_df, lineitem_df], axis=1, sort=False)

# Perform the filtering according to the SQL query
filtered_data = merged_data[(merged_data['R_NAME'] == 'ASIA') &
                            (merged_data['O_ORDERDATE'] >= datetime(1990, 1, 1)) &
                            (merged_data['O_ORDERDATE'] < datetime(1995, 1, 1))]

# Perform the aggregation
grouped_data = filtered_data.groupby('N_NAME', as_index=False).agg({'L_EXTENDEDPRICE': 'sum'})

# Create the REVENUE column
grouped_data['REVENUE'] = grouped_data['L_EXTENDEDPRICE'] * (1 - grouped_data['L_DISCOUNT'])

# Sort the data in descending order of REVENUE
final_data = grouped_data.sort_values('REVENUE', ascending=False)

# Write the final output to a CSV file
final_data.to_csv('query_output.csv', index=False)
