import pandas as pd
import mysql.connector
from pymongo import MongoClient

# Setting up MySQL connection
mydb = mysql.connector.connect(host="mysql", user="root", passwd="my-secret-pw", database="tpch")
mycursor = mydb.cursor()

# Setting up MongoDB connection
client = MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]

# reading all the data from MongoDB collections
partsupp_data = pd.DataFrame(list(mongodb.partsupp.find()))
customer_data = pd.DataFrame(list(mongodb.customer.find()))
orders_data = pd.DataFrame(list(mongodb.orders.find()))
lineitem_data = pd.DataFrame(list(mongodb.lineitem.find()))

# Query on MongoDB dataframes
df_lineitem_group = lineitem_data.groupby("L_ORDERKEY")["L_QUANTITY"].sum().reset_index()
df_lineitem_group = df_lineitem_group[df_lineitem_group["L_QUANTITY"] > 300]

merged_data = pd.merge(customer_data, orders_data, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
merged_data = pd.merge(merged_data, lineitem_data, on="L_ORDERKEY")
merged_data = merged_data[merged_data["O_ORDERKEY"].isin(df_lineitem_group["L_ORDERKEY"])]

final_data = merged_data.groupby(["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE"])["L_QUANTITY"].sum().reset_index()
final_data = final_data.sort_values(by=["O_TOTALPRICE", "O_ORDERDATE"], ascending=[False, True])

final_data.to_csv('query_output.csv')
