import csv
import mysql.connector

config = {
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql",
    "database": "tpch",
    "raise_on_warnings": True,
}

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

query = """
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
"""

cursor.execute(query)

with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([i[0] for i in cursor.description])  # write headers
    writer.writerows(cursor.fetchall())  # write data

cursor.close()
cnx.close()
