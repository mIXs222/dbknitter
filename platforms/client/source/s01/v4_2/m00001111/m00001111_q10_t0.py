import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient
import csv

# Connection to MySQL Server
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch', charset='utf8mb4', cursorclass=pymysql.cursors.Cursor)
cursor = mysql_connection.cursor()

mysql_query = "SELECT N_NATIONKEY, N_NAME FROM nation"
cursor.execute(mysql_query)
mysql_results = cursor.fetchall()

# Create a pandas DataFrame for nation
nation_df = pd.DataFrame(mysql_results, columns=["N_NATIONKEY", "N_NAME"])
mysql_connection.close()

# Connection to MongoDB Server
mongodb_client = MongoClient("mongodb", 27017)
mongodb_database = mongodb_client["tpch"]

# Queries for customer, orders, lineitem
customer_data = mongodb_database.customer.find({}, {"C_CUSTKEY": 1, "C_NAME": 1, "C_ACCTBAL": 1, "C_PHONE": 1,
                                                    "C_ADDRESS": 1, "C_NATIONKEY": 1, "C_COMMENT": 1})
orders_data = mongodb_database.orders.find({"O_ORDERDATE": {"$gte": "1993-10-01", "$lt": "1994-01-01"}},  
                                           {"O_ORDERKEY": 1, "O_CUSTKEY": 1})
lineitem_data = mongodb_database.lineitem.find({"L_RETURNFLAG": "R"}, 
                                               {"L_ORDERKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1})

# Create a pandas DataFrames for customer, orders, lineitem
customer_df = pd.DataFrame(list(customer_data))
orders_df = pd.DataFrame(list(orders_data))
lineitem_df = pd.DataFrame(list(lineitem_data))

# Merge tables
temp_df = pd.merge(customer_df, orders_df, how='inner', left_on="C_CUSTKEY", right_on="O_CUSTKEY")
temp_df = pd.merge(temp_df, lineitem_df, how='inner', left_on="O_ORDERKEY", right_on="L_ORDERKEY")
final_df = pd.merge(temp_df, nation_df, how='inner', left_on="C_NATIONKEY", right_on="N_NATIONKEY")

# Compute REVENUE
final_df["REVENUE"] = final_df["L_EXTENDEDPRICE"] * (1 - final_df["L_DISCOUNT"])

# Group by and sorting
group_df = final_df.groupby(["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_PHONE", "N_NAME", "C_ADDRESS", "C_COMMENT"]).\
            agg(REVENUE=pd.NamedAgg(column='REVENUE', aggfunc=sum)).reset_index()
sorted_df = group_df.sort_values(['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Save result to csv
sorted_df.to_csv('query_output.csv', index=False)
