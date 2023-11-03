import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
import numpy as np

# Initialising the Mongo client
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Initialising the MySQL client
mysql_con = create_engine("mysql+pymysql://root:my-secret-pw@mysql/tpch")

# Querying the data from MongoDB
customer_data = pd.DataFrame(list(mongo_db['customer'].find()))
orders_data = pd.DataFrame(list(mongo_db['orders'].find()))

# Renaming column names to what's expected in the query
customer_data.columns = customer_data.columns.str.upper()
orders_data.columns = orders_data.columns.str.upper()

# Perform the operations in the sub-query
customer_data['CNTRYCODE'] = customer_data['C_PHONE'].str.slice(0, 2)
selected_countries = ['20', '40', '22', '30', '39', '42', '21']
customer_data = customer_data[customer_data['CNTRYCODE'].isin(selected_countries)]
average_acctbal = customer_data[customer_data['C_ACCTBAL'] > 0]['C_ACCTBAL'].mean()
customer_data = customer_data[customer_data['C_ACCTBAL'] > average_acctbal]

# Exclude customers who have orders
excluded_keys = orders_data['O_CUSTKEY'].unique()
customer_data = customer_data[~customer_data['C_CUSTKEY'].isin(excluded_keys)]

# Perform the GROUP BY and COUNT(*) and SUM operations
output = customer_data.groupby('CNTRYCODE').agg(
    NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
    TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
).reset_index()

# Perform the ORDER BY operation
output = output.sort_values(by='CNTRYCODE')

# Write the output to CSV file
output.to_csv('query_output.csv', index=False)
