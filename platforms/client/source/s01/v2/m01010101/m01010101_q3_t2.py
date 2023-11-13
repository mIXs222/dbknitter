import mysql.connector
import pandas as pd
import pymongo
from pymongo import MongoClient
from pandas import DataFrame
import csv

# MySQL Connection
mysql_conn = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# MongoDB Connection
client = MongoClient("mongodb://mongodb:27017/")
mongodb = client['tpch']

# MySQL Query Execution
mysql_df = pd.read_sql_query(
'''
SELECT
 orders.O_ORDERKEY,
 orders.O_ORDERDATE,
 orders.O_SHIPPRIORITY
FROM
 orders 
WHERE
 orders.O_ORDERDATE < '1995-03-15' 
''', mysql_conn)

# MongoDB Query Execution
customer_df = pd.DataFrame(list(mongodb.customer.find({"C_MKTSEGMENT":"BUILDING"})))
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find({"L_SHIPDATE":{'$gt':'1995-03-15'}})))

# Merging the three dataframes
df1 = pd.merge(mysql_df, customer_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df2 = pd.merge(df1, lineitem_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Applying calculations and group by
df2['REVENUE']=df2['L_EXTENDEDPRICE']*(1-df2['L_DISCOUNT'])
final_df = df2.groupby(['L_ORDERKEY','O_ORDERDATE','O_SHIPPRIORITY']).sum().reset_index()

# Sorting by REVENUE and O_ORDERDATE
final_df.sort_values(by=['REVENUE','O_ORDERDATE'], ascending=[False,True], inplace=True)

# Writing output to csv file
final_df.to_csv('query_output.csv', index=False)

print("Query results are written in query_output.csv file")
