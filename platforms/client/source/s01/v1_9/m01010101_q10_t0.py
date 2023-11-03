from pymongo import MongoClient
from mysql.connector import connect, Error
import pandas as pd
from pandas.io.json import json_normalize
import csv

# MySQL connection
mysql_conn = connect(
    host="mysql",
    database="tpch",
    user="root",
    password="my-secret-pw")

# MongoDB connection
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]


def get_mongo_data(collection_name):
    collection = mongo_db[collection_name]
    return json_normalize(list(collection.find()))


def get_sql_data(query):
    with mysql_conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


# Fetch required data from MongoDB 
customer_data = get_mongo_data("customer")
order_data = get_mongo_data("orders")
nation_data = get_mongo_data("nation")
lineitem_data = get_mongo_data("lineitem")

# Prepare DataFrame for SQL like operations
df_customer = pd.DataFrame(customer_data)
df_order = pd.DataFrame(order_data)
df_nation = pd.DataFrame(nation_data)
df_lineitem = pd.DataFrame(lineitem_data)

# Merge datasets 
merged_customers_orders = pd.merge(df_customer, df_order, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_lineitem = pd.merge(merged_customers_orders, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
final_df = pd.merge(merged_lineitem, df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Perform query operations
final_df = final_df[
    (final_df['O_ORDERDATE'] >= '1993-10-01') & (final_df['O_ORDERDATE'] < '1994-01-01') & (final_df['L_RETURNFLAG'] == 'R')
]
final_df['REVENUE'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])
final_df = final_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT']).sum().reset_index()

# Order by REVENUE, C_CUSTKEY, C_NAME, C_ACCTBAL
final_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[1, 1, 1, 0], inplace=True)

# Export resulting DataFrame to CSV file
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
