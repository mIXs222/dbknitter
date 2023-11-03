import mysql.connector
import pymongo
import pandas as pd
from io import StringIO

# Connect to MySQL
mysql_db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mysql_cursor = mysql_db.cursor()

query_mysql = """
SELECT N_NAME AS NATION, S_SUPPKEY, S_NATIONKEY, P_PARTKEY, P_NAME 
FROM NATION, SUPPLIER, PART 
WHERE S_NATIONKEY = N_NATIONKEY AND P_NAME LIKE '%dim%'
"""

mysql_cursor.execute(query_mysql)

data_mysql = pd.DataFrame(mysql_cursor.fetchall(), columns=["NATION", "S_SUPPKEY", "S_NATIONKEY", "P_PARTKEY", "P_NAME"])

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongo_db = client["tpch"]

data_partsupp = pd.DataFrame(list(mongo_db.partsupp.find({}, {"_id": 0})))
data_lineitem = pd.DataFrame(list(mongo_db.lineitem.find({}, {"_id": 0})))
data_orders = pd.DataFrame(list(mongo_db.orders.find({}, {"_id": 0})))

# Merge all data
df = pd.merge(data_mysql, data_partsupp, how='left', left_on=["S_SUPPKEY", "P_PARTKEY"], right_on=["PS_SUPPKEY", "PS_PARTKEY"])
df = pd.merge(df, data_lineitem, how='left', left_on=["P_PARTKEY", "S_SUPPKEY"], right_on=["L_PARTKEY", "L_SUPPKEY"])
df = pd.merge(df, data_orders, how='left', on="O_ORDERKEY")

df['O_YEAR'] = pd.to_datetime(df['O_ORDERDATE']).dt.strftime('%Y')
df['AMOUNT'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT']) - df['PS_SUPPLYCOST'] * df['L_QUANTITY'] 
df_grouped = df.groupby(['NATION','O_YEAR'])['AMOUNT'].sum().reset_index(name='SUM_PROFIT')

df_sorted = df_grouped.sort_values(['NATION', 'O_YEAR'], ascending=[True, False])

# Write to CSV
df_sorted.to_csv("query_output.csv", index=False)
