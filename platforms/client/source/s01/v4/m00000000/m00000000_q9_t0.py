import pymysql
import csv

# Database connection parameters
db_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# SQL Query
query = """
SELECT
    N.N_NAME AS NATION,
    EXTRACT(YEAR FROM O.O_ORDERDATE) AS O_YEAR,
    SUM(L.L_EXTENDEDPRICE * (1 - L.L_DISCOUNT) - PS.PS_SUPPLYCOST * L.L_QUANTITY) AS SUM_PROFIT
FROM
    part P,
    supplier S,
    lineitem L,
    partsupp PS,
    orders O,
    nation N
WHERE
    S.S_SUPPKEY = L.L_SUPPKEY
    AND PS.PS_SUPPKEY = L.L_SUPPKEY
    AND PS.PS_PARTKEY = L.L_PARTKEY
    AND P.P_PARTKEY = L.L_PARTKEY
    AND O.O_ORDERKEY = L.L_ORDERKEY
    AND S.S_NATIONKEY = N.N_NATIONKEY
    AND P.P_NAME LIKE '%dim%'
GROUP BY
    N.N_NAME,
    O_YEAR
ORDER BY
    N.N_NAME,
    O_YEAR DESC
"""

# Prepare the CSV file to write the results
csv_file = 'query_output.csv'

# Connect to the database
conn = pymysql.connect(**db_config)
try:
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

        # Write results to CSV
        with open(csv_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['NATION', 'O_YEAR', 'SUM_PROFIT'])
            for row in results:
                writer.writerow(row)

finally:
    conn.close()
