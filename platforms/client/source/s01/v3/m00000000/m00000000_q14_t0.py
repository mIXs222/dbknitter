import csv
import mysql.connector

db_connection = mysql.connector.connect(host="mysql", user="root", password="my-secret-pw", database="tpch")

cursor = db_connection.cursor()

cursor.execute("""
SELECT
    100.00 * SUM(CASE WHEN P_TYPE LIKE 'PROMO%' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT)
    ELSE 0
    END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE
FROM
    lineitem,
    part
WHERE
    L_PARTKEY = P_PARTKEY
    AND L_SHIPDATE >= '1995-09-01'
    AND L_SHIPDATE < '1995-10-01'
""")

result_set = cursor.fetchall()

with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(result_set)
