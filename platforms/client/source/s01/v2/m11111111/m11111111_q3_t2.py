import os
import pandas as pd
from pymongo import MongoClient
import mysql.connector

# Create connection to MongoDB
mongo_client = MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client['tpch']

# Define a function to make a dataframe for MongoDB collections
def pandas_dataframe(mongo_db, collection_name):
    collection = mongo_db[collection_name]
    data = pd.DataFrame(list(collection.find()))
    return data

# Create dataframes for each collection in MongoDB
mongo_customer_df = pandas_dataframe(mongo_db, 'customer')
mongo_orders_df = pandas_dataframe(mongo_db, 'orders')
mongo_lineitem_df = pandas_dataframe(mongo_db, 'lineitem')

# Create MySQL connection
mysql_db = mysql.connector.connect(host="localhost", user="yourusername", password="yourpassword", database="yourdatabase")

# Execute SQL query
mysql_cursor = mysql_db.cursor(dictionary=True)
query = """
    SELECT
        L_ORDERKEY,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
        O_ORDERDATE,
        O_SHIPPRIORITY
    FROM
        customer,
        orders,
        lineitem
    WHERE
        C_MKTSEGMENT = 'BUILDING'
        AND C_CUSTKEY = O_CUSTKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_ORDERDATE < '1995-03-15'
        AND L_SHIPDATE > '1995-03-15'
    GROUP BY
        L_ORDERKEY,
        O_ORDERDATE,
        O_SHIPPRIORITY
    ORDER BY
        REVENUE DESC,
        O_ORDERDATE
"""
mysql_cursor.execute(query)
mysql_df = pd.DataFrame(mysql_cursor.fetchall())

# Get the intersection of MongoDB and MySQL data
intersection_df = pd.merge(mongo_customer_df, mongo_orders_df, mongo_lineitem_df, mysql_df)

# Write the result to query_output.csv
intersection_df.to_csv("query_output.csv", index=False)
