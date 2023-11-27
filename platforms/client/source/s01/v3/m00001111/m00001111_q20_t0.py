import mysql.connector
from pymongo import MongoClient
import pandas as pd
import csv

# setup mysql connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', 
database='tpch')

# setup mongodb connection
client = MongoClient('mongodb', 27017)
db = client['tpch']

# setup mysql cursor
cur = cnx.cursor()

# execute mysql query
mysql_query = """
    SELECT
    S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY
    FROM
        supplier, nation
    WHERE S_NATIONKEY = N_NATIONKEY AND N_NAME = 'CANADA'"""

cur.execute(mysql_query)

# fetch mysql query results
mysql_data = cur.fetchall()

# setup variables
suppliers = {}
for row in mysql_data:
    suppliers[row[0]] = {"S_NAME": row[1], "S_ADDRESS": row[2], "S_NATIONKEY": row[3]}

# get partsupp from mongodb
partsupp = db.partsupp.find()

# variables
partkeys = []

# execute mongodb query
for row in partsupp:
    if row['PS_PARTKEY'] not in partkeys and 'PS_SUPPKEY' in row:
        partkeys.append(row['PS_PARTKEY'])

# get part from mysql
part_query = "SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'"

cur.execute(part_query)

# fetch part query results
part_data = cur.fetchall()

# check if partkeys in part_data
for part in part_data:
    if part[0] not in partkeys:
        partkeys.remove(part[0])

# get lineitem from mongodb
lineitem = db.lineitem.find()

total_qty = 0
for row in lineitem:
    if row['L_PARTKEY'] in partkeys and row['L_SHIPDATE'] >= '1994-01-01' and row['L_SHIPDATE'] < '1995-01-01':
        total_qty += row['L_QUANTITY']

# check PS_AVAILQTY > 0.5 * total_qty
partsupp = db.partsupp.find()
for row in partsupp:
    if row['PS_PARTKEY'] in partkeys and row['PS_AVAILQTY'] > 0.5 * total_qty:
        if row['PS_SUPPKEY'] in suppliers:
            with open('query_output.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([suppliers[row['PS_SUPPKEY']]['S_NAME'], suppliers[row['PS_SUPPKEY']]['S_ADDRESS']])

cnx.close()
