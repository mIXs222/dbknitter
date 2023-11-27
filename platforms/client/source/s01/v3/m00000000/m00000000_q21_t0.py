import mysql.connector
import csv

cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql', database='tpch')

cursor = cnx.cursor()

query = (
    "SELECT "
    "S_NAME, "
    "COUNT(*) AS NUMWAIT "
    "FROM "
    "supplier, "
    "lineitem AS L1, "
    "orders, "
    "nation "
    "WHERE "
    "S_SUPPKEY = L1.L_SUPPKEY "
    "AND O_ORDERKEY = L1.L_ORDERKEY "
    "AND O_ORDERSTATUS = 'F' "
    "AND L1.L_RECEIPTDATE > L1.L_COMMITDATE "
    "AND EXISTS ("
        "SELECT "
        "* "
        "FROM "
        "lineitem AS L2 "
        "WHERE "
        "L2.L_ORDERKEY = L1.L_ORDERKEY "
        "AND L2.L_SUPPKEY <> L1.L_SUPPKEY) "
    "AND NOT EXISTS ("
        "SELECT "
        "* "
        "FROM "
        "lineitem AS L3 "
        "WHERE "
        "L3.L_ORDERKEY = L1.L_ORDERKEY "
        "AND L3.L_SUPPKEY <> L1.L_SUPPKEY "
        "AND L3.L_RECEIPTDATE > L3.L_COMMITDATE) "
    "AND S_NATIONKEY = N_NATIONKEY "
    "AND N_NAME = 'SAUDI ARABIA' "
    "GROUP BY "
    "S_NAME "
    "ORDER BY "
    "NUMWAIT DESC, "
    "S_NAME")

cursor.execute(query)

with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(cursor)

cursor.close()
cnx.close()
