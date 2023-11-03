import mysql.connector
import pymongo
import pandas as pd

# Getting data from MySQL
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM customer")
customer = pd.DataFrame(mycursor.fetchall(), columns=["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"])

mycursor.execute("SELECT * FROM orders")
orders = pd.DataFrame(mycursor.fetchall(), columns=["O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"])

mydb.close()

# Getting data from MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

lineitem = pd.DataFrame(list(db.lineitem.find()), columns=["L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE","L_COMMENT"])

# Performing query operation
grouped_lineitem = lineitem.groupby('L_ORDERKEY').sum().reset_index()
filtered_lineitem = grouped_lineitem[grouped_lineitem['L_QUANTITY'] > 300][['L_ORDERKEY', 'L_QUANTITY']]

merged_df = pd.merge(customer, orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, filtered_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

grouped_df = merged_df.groupby(['C_NAME','C_CUSTKEY','O_ORDERKEY','O_ORDERDATE', 'O_TOTALPRICE'])['L_QUANTITY'].sum().reset_index()

result = grouped_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])
result.to_csv('query_output.csv')

