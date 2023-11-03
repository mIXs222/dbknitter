import mysql.connector
import pandas as pd
import csv

# create a connection to the database
cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                                  host='mysql',
                                  database='tpch')
cursor = cnx.cursor()

# execute the query
query = """
    SELECT
        C_COUNT,
        COUNT(*) AS CUSTDIST
    FROM
        (
        SELECT
            C_CUSTKEY,
            COUNT(O_ORDERKEY) AS C_COUNT
        FROM
            customer LEFT OUTER JOIN orders ON
            C_CUSTKEY = O_CUSTKEY
            AND O_COMMENT NOT LIKE '%pending%deposits%'
        GROUP BY
            C_CUSTKEY
        )   C_ORDERS
    GROUP BY
        C_COUNT
    ORDER BY
        CUSTDIST DESC,
        C_COUNT DESC
"""
cursor.execute(query)

# fetch all the results
rows = cursor.fetchall()

# write the results to a csv file
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(rows)
    
# close the cursor and connection
cursor.close()
cnx.close()
