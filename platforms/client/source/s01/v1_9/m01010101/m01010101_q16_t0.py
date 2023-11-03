import mysql.connector
import pandas as pd
from pymongo import MongoClient

# connect to mysql
mysql_db = mysql.connector.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_db.cursor()

# connect to mongodb
client = MongoClient("mongodb://localhost:27017/")
mongodb = client['tpch']

# execute subquery on mongodb
subquery_result = mongodb.supplier.find({"S_COMMENT": {'$regex':'Customer.*Complaints'}}, {"S_SUPPKEY": 1})

# get all 'S_SUPPKEY' values from the subquery results
excluded_suppkeys = [item['S_SUPPKEY'] for item in subquery_result]

# MYSQL Query
mysql_query = """
SELECT
    P_BRAND,
    P_TYPE,
    P_SIZE,
    COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM
    partsupp,
    part
WHERE
    P_PARTKEY = PS_PARTKEY
    AND P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND PS_SUPPKEY NOT IN ({})
GROUP BY
    P_BRAND,
    P_TYPE,
    P_SIZE
ORDER BY
    SUPPLIER_CNT DESC,
    P_BRAND,
    P_TYPE,
    P_SIZE
""".format(','.join(map(str,excluded_suppkeys))) # converting list of suppkeys to csv and inserting in query

# executing the MYSQL query
mysql_cursor.execute(mysql_query)

# fetch results and write it to csv file
result = mysql_cursor.fetchall()

# Create a pandas dataframe from SQL query result 
df = pd.DataFrame(result, columns=["P_BRAND", "P_TYPE", "P_SIZE", "SUPPLIER_CNT"])
df.to_csv('query_output.csv', index=False) # write the dataframe to CSV file
