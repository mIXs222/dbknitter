import csv
import mysql.connector
from pymongo import MongoClient
import pandas as pd

# Setup the connections
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mycursor = mydb.cursor()
client = MongoClient('mongodb', 27017)
mydb_mongo = client['tpch']
supplier = mydb_mongo['supplier']
lineitem = mydb_mongo['lineitem']

# Execute Mysql Queries

mysql_query1 = """SELECT PS_SUPPKEY
                  FROM partsupp 
                  WHERE PS_PARTKEY IN 
                      (SELECT P_PARTKEY 
                       FROM part 
                       WHERE P_NAME LIKE 'forest%')"""
mycursor.execute(mysql_query1)
suppKeys = [x[0] for x in mycursor.fetchall()]


mysql_query2 = """SELECT N_NATIONKEY
                  FROM nation 
                  WHERE N_NAME = 'CANADA'"""
mycursor.execute(mysql_query2)
nationKeys = [x[0] for x in mycursor.fetchall()]

# Execute Mongo Queries

mongo_query1 = {"S_SUPPKEY": {"$in": suppKeys}, "S_NATIONKEY": {"$in": nationKeys}}
supplier_df = pd.DataFrame(list(supplier.find(mongo_query1)))

mongo_query2 = {"L_PARTKEY": {"$in": suppKeys}, "L_SUPPKEY": {"$in": suppKeys}, 
                "L_SHIPDATE": {"$gte": "1994-01-01", "$lt": "1995-01-01"}}
lineitem_df = pd.DataFrame(list(lineitem.find(mongo_query2)))

lineitem_agg = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()
lineitem_agg.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'sum_quan']
lineitem_agg['sum_quan'] = lineitem_agg['sum_quan'] * 0.5

# Join Dataframes

output = pd.merge(supplier_df, lineitem_agg, how='inner', left_on=[ 'S_SUPPKEY'], right_on=['PS_SUPPKEY'])
output = output[output['sum_quan'] > output['PS_AVAILQTY']]
output[['S_NAME', 'S_ADDRESS']].to_csv('query_output.csv', index=False)
