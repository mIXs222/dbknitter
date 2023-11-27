import csv
import mysql.connector

db_connection = mysql.connector.connect(
    host='mysql',
    database='tpch',
    user='root',
    password='my-secret-pw'
)

cursor = db_connection.cursor()

query = """
SELECT
    S_ACCTBAL,
    S_NAME,
    N_NAME,
    P_PARTKEY,
    P_MFGR,
    S_ADDRESS,
    S_PHONE,
    S_COMMENT
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    P_PARTKEY = PS_PARTKEY
    AND S_SUPPKEY = PS_SUPPKEY
    AND P_SIZE = 15
    AND P_TYPE LIKE '%BRASS'
    AND S_NATIONKEY = N_NATIONKEY
    AND N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'EUROPE'
    AND PS_SUPPLYCOST = (
        SELECT
            MIN(PS_SUPPLYCOST)
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            P_PARTKEY = PS_PARTKEY
            AND S_SUPPKEY = PS_SUPPKEY
            AND S_NATIONKEY = N_NATIONKEY
            AND N_REGIONKEY = R_REGIONKEY
            AND R_NAME = 'EUROPE'
        )
ORDER BY
    S_ACCTBAL DESC,
    N_NAME,
    S_NAME,
    P_PARTKEY
"""

cursor.execute(query)

with open("query_output.csv", "w", newline='') as file:
    writer = csv.writer(file)
    writer.writerows(cursor.fetchall())
cursor.close()
db_connection.close()
