# python code (query.py)

import mysql.connector
import pymongo
import pandas as pd

# Connect to mysql database
mysql_db = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)
mysql_cursor = mysql_db.cursor()

# mysql queries
mysql_query_nation = "SELECT * FROM nation WHERE N_NAME = 'SAUDI ARABIA'"
mysql_query_orders = "SELECT * FROM orders WHERE O_ORDERSTATUS = 'F'"

mysql_cursor.execute(mysql_query_nation)
nation_data = mysql_cursor.fetchall()
nation_df = pd.DataFrame(nation_data)

mysql_cursor.execute(mysql_query_orders)
orders_data = mysql_cursor.fetchall()
orders_df = pd.DataFrame(orders_data)

mysql_db.close()

# connect to mongodb
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]

# mongodb queries
supplier_data = mongodb_db["supplier"].find({})
supplier_df = pd.DataFrame(supplier_data)

lineitem_data = mongodb_db["lineitem"].find({})
lineitem_df = pd.DataFrame(lineitem_data)

# merging data
merge1 = pd.merge(lineitem_df, supplier_df, how="inner", left_on="L_SUPPKEY", right_on="S_SUPPKEY")
merge2 = pd.merge(merge1, orders_df, how="inner", left_on="L_ORDERKEY", right_on="O_ORDERKEY")
final_merge = pd.merge(merge2, nation_df, how="inner", left_on="S_NATIONKEY", right_on="N_NATIONKEY")

# applying conditions and aggregates function

# rest of the operation according to query here
# And finally output the query result like this:

query_output = ... # replace '...' with your final result after all operations done as per your query

# Export to csv
query_output.to_csv("query_output.csv")
