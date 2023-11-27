import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis
import csv
import os

# Connect to mysql
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')
cursor = connection.cursor()

# Execute the query
query = '''
SELECT
    part.P_BRAND,
    part.P_TYPE,
    part.P_SIZE,
    COUNT(DISTINCT partsupp.PS_SUPPKEY) AS SUPPLIER_CNT
FROM
    partsupp
JOIN
    part ON part.P_PARTKEY = partsupp.PS_PARTKEY
WHERE
    part.P_BRAND <> 'Brand#45'
    AND part.P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND part.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND partsupp.PS_SUPPKEY NOT IN (
        SELECT
            supplier.S_SUPPKEY
        FROM
            supplier
        WHERE
            supplier.S_COMMENT LIKE '%Customer%Complaints%'
    )
GROUP BY
    part.P_BRAND,
    part.P_TYPE,
    part.P_SIZE
ORDER BY
    SUPPLIER_CNT DESC,
    part.P_BRAND,
    part.P_TYPE,
    part.P_SIZE
'''
cursor.execute(query)

# Fetch all the records
tuples = cursor.fetchall()

# Write the output to csv file
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])  # writing headers
    writer.writerows(tuples)
