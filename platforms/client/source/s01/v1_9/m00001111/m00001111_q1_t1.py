from pymongo import MongoClient
from bson.code import Code
import pandas as pd
import MySQLdb

# mongodb connection
client = MongoClient("mongodb://localhost:27017/")
mongodb = client["tpch"]

# mysql connection
db=MySQLdb.connect(host="mysql",user="root",passwd="my-secret-pw",db="tpch")
cursor=db.cursor()

# Copy mongodb data to mysql
lineitems = mongodb["lineitem"].find()
for item in lineitems:
    values = []
    for field in ['L_RETURNFLAG','L_LINESTATUS','L_QUANTITY','L_EXTENDEDPRICE','L_DISCOUNT','L_TAX']:
        values.append(item[field])
    query = "INSERT INTO lineitem (L_RETURNFLAG,L_LINESTATUS,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(query, values)

# Execute query in mysql
query = """
    SELECT
        L_RETURNFLAG,
        L_LINESTATUS,
        SUM(L_QUANTITY) AS SUM_QTY,
        SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
        AVG(L_QUANTITY) AS AVG_QTY,
        AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
        AVG(L_DISCOUNT) AS AVG_DISC,
        COUNT(*) AS COUNT_ORDER
    FROM
        lineitem
    WHERE
        L_SHIPDATE <= '1998-09-02'
    GROUP BY
        L_RETURNFLAG,
        L_LINESTATUS
    ORDER BY
        L_RETURNFLAG,
        L_LINESTATUS;
"""
cursor.execute(query)
result = cursor.fetchall()

# Write result to csv
df = pd.DataFrame(result)
df.to_csv('query_output.csv', index=False)
