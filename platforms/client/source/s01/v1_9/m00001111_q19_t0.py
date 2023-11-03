import pandas as pd
import numpy as np
from pandasql import sqldf
from pymongo import MongoClient
from mysql.connector import connect
import csv

# connecting to the mysql server
cnx = connect(user='root', password='my-secret-pw',
              host='mysql', database='tpch')

# creating a cursor object
cursor = cnx.cursor()

# fetching data from the PART table
query = ("SELECT * FROM PART")
cursor.execute(query)

# putting fetched data into a pandas dataframe
part_df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

# connecting to the mongodb server
client = MongoClient('mongodb', 27017)

# accessing the tpch database
db = client.tpch

# fetching data from the lineitem collection
cursor = db.lineitem.find()

# putting fetched data into a pandas dataframe
lineitem_df = pd.DataFrame(list(cursor))

# defining a function to execute sql on pandas dataframes
pysqldf = lambda q: sqldf(q, globals())

# executing the query and storing the result in a dataframe
result = pysqldf("""
    SELECT
        SUM(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) AS REVENUE
    FROM
        lineitem_df as lineitem,
        part_df as part
    WHERE
        (
        P_PARTKEY = L_PARTKEY
        AND P_BRAND = 'Brand#12'
        AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND L_QUANTITY >= 1 AND L_QUANTITY <= 1 + 10
        AND P_SIZE BETWEEN 1 AND 5
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
        )
        OR
        (
        P_PARTKEY = L_PARTKEY
        AND P_BRAND = 'Brand#23'
        AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND L_QUANTITY >= 10 AND L_QUANTITY <= 10 + 10
        AND P_SIZE BETWEEN 1 AND 10
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
        )
        OR
        (
        P_PARTKEY = L_PARTKEY
        AND P_BRAND = 'Brand#34'
        AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND L_QUANTITY >= 20 AND L_QUANTITY <= 20 + 10
        AND P_SIZE BETWEEN 1 AND 15
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
        )
""")

# Closing the MySQL connection
cnx.close()
# Closing the MongoDB connection
client.close()

# Writing the output to CSV file
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
