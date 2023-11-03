import mysql.connector
import pandas as pd
from pymongo import MongoClient

# MySQL Connection
mysql_db = mysql.connector.connect(
    host="mysql",
    user="root",
    passwd="my-secret-pw",
    database="tpch"
)

mysql_cursor = mysql_db.cursor()

# MongoDB Connection
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Get data from MySQL
mysql_cursor.execute("SELECT * FROM NATION")
nation = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

mysql_cursor.execute("SELECT * FROM REGION")
region = pd.DataFrame(mysql_cursor.fetchall(), columns=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])

mysql_cursor.execute("SELECT * FROM PART")
part = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

mysql_cursor.execute("SELECT * FROM SUPPLIER")
supplier = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Get data from MongoDB
partsupp = pd.DataFrame(list(mongo_db.partsupp.find()))
customer = pd.DataFrame(list(mongo_db.customer.find()))
orders = pd.DataFrame(list(mongo_db.orders.find()))
lineitem = pd.DataFrame(list(mongo_db.lineitem.find()))

# Execute Query
mask = ((lineitem['L_SHIPDATE'] >= '1994-01-01') & 
        (lineitem['L_SHIPDATE'] < '1995-01-01') & 
        (lineitem['L_DISCOUNT'] >= .06 - 0.01) & 
        (lineitem['L_DISCOUNT'] <= .06 + 0.01) & 
        (lineitem['L_QUANTITY'] < 24))

query_output = lineitem.loc[mask, ['L_EXTENDEDPRICE', 'L_DISCOUNT']]
query_output['REVENUE'] = query_output['L_EXTENDEDPRICE'] * query_output['L_DISCOUNT']

# Write output to csv
query_output.to_csv('query_output.csv', index=False)
