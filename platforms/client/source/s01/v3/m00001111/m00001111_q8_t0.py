import csv
import mysql.connector
import pymongo
from pymongo import MongoClient
from itertools import chain

# Connect to MySQL
mydb = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

# Execute MySQL query
mycursor = mydb.cursor()
mycursor.execute("""
    SELECT
        P_PARTKEY,
        S_SUPPKEY,
        S_NATIONKEY,
        S_NAME
    FROM
        part,
        supplier
    WHERE
        P_TYPE = 'SMALL PLATED COPPER'
""")
mysql_data = mycursor.fetchall()

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Execute MongoDB query

# Get customer and nation data
customer_data = db['customer'].find({})
nation_data = db['nation'].find({})

# Join MongoDB & MySQL data based on keys
result = []
for mysql_row in mysql_data:
    for customer_row in customer_data:
        if mysql_row[2] == customer_row['C_NATIONKEY']:
            for nation_row in nation_data:
                if nation_row['N_NATIONKEY'] == mysql_row[2]:
                    result.append(list(chain(mysql_row, customer_row.values(), nation_row.values())))

# Write the result into CSV file
with open("query_output.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(result)
