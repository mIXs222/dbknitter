import pymongo
from pymongo import MongoClient
import pandas as pd
import mysql.connector

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
collection_part = db['part']
collection_lineitem = db['lineitem']

# Get data from MongoDB
part_data = pd.DataFrame(list(collection_part.find()))
lineitem_data = pd.DataFrame(list(collection_lineitem.find()))

# Connection to mysql
db_mysql = mysql.connector.connect(
    host="localhost",
    user="user",
    password="password",
    database="tpch"
)

cursor = db_mysql.cursor()

# Execute MySQL query
cursor.execute("""
SELECT
    SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
FROM
    lineitem,
    part
WHERE
    P_PARTKEY = L_PARTKEY
    AND P_BRAND = 'Brand#23'
    AND P_CONTAINER = 'MED BAG'
    AND L_QUANTITY < (
        SELECT
            0.2 * AVG(L_QUANTITY)
        FROM
            lineitem
        WHERE
            L_PARTKEY = P_PARTKEY
    )
""")

# Fetch all rows from the last executed statement
mysql_data = cursor.fetchall()
mysql_df = pd.DataFrame(mysql_data, columns=['AVG_YEARLY'])

# Save the result to csv
mysql_df.to_csv('query_output.csv', index=False)
