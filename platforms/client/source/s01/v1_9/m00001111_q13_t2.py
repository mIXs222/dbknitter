import mysql.connector
import pandas as pd
import os
from pymongo import MongoClient

def mysql_to_df(con, query):
    df = pd.read_sql(query, con)
    return df

def mongo_to_df(db, collection_name):
    collection = db[collection_name]
    data = pd.DataFrame(list(collection.find()))
    return data

# Connect to MySQL and fetch data
mysql_con = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
customer_df = mysql_to_df(mysql_con, "SELECT * FROM customer")
orders_df = mysql_to_df(mysql_con, "SELECT * FROM orders")

mysql_con.close()

# Connect to MongoDB and fetch data
mongo_client = MongoClient('mongodb', 27017)
db = mongo_client['tpch']

partsupp_df = mongo_to_df(db, 'partsupp')
customer_df_mongo = mongo_to_df(db, 'customer')
orders_df_mongo = mongo_to_df(db, 'orders')
lineitem_df = mongo_to_df(db, 'lineitem')

# Merge MySQL and MongoDB data
customer_df = pd.concat([customer_df, customer_df_mongo], ignore_index=True)
orders_df = pd.concat([orders_df, orders_df_mongo], ignore_index=True)

# Perform the SQL operation
merged_df = pd.merge(customer_df, orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df[~merged_df['O_COMMENT'].str.contains('pending deposits', na=False)]
grp_df = merged_df.groupby('C_CUSTKEY').size().reset_index(name='C_COUNT')

final_df = grp_df.groupby('C_COUNT').size().reset_index(name='CUSTDIST')
final_df = final_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False])

# Write Result
final_df.to_csv('query_output.csv', index=False)
