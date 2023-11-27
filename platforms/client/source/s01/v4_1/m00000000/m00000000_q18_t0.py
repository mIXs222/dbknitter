import pandas as pd
import pymysql
import pymongo
import direct_redis
import csv

# Connect to MySQL DB
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = conn.cursor(pymysql.cursors.DictCursor)

MySQL_query = """
SELECT
    C_NAME,
    C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE,
    SUM(L_QUANTITY)
FROM
    customer,
    orders,
    lineitem
WHERE
    O_ORDERKEY IN (
    SELECT
        L_ORDERKEY
    FROM
        lineitem
    GROUP BY
    L_ORDERKEY HAVING
        SUM(L_QUANTITY) > 300
    )
AND C_CUSTKEY = O_CUSTKEY
AND O_ORDERKEY = L_ORDERKEY
GROUP BY
    C_NAME,
    C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE
ORDER BY
    O_TOTALPRICE DESC,
    O_ORDERDATE
"""
cursor.execute(MySQL_query)
result = cursor.fetchall()

# Write results to CSV
keys = result[0].keys()
with open('query_output.csv', 'w', newline='')  as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(result)
