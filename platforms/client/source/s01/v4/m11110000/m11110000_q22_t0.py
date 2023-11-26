import pymysql
import csv

# MySQL connection parameters
params = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch',
}

# Connect to MySQL
cnx = pymysql.connect(**params)
cursor = cnx.cursor()

# Perform query
query = """
SELECT
    CNTRYCODE,
    COUNT(*) AS NUMCUST,
    SUM(C_ACCTBAL) AS TOTACCTBAL
FROM
    (
    SELECT
        SUBSTR(C_PHONE, 1 , 2) AS CNTRYCODE,
        C_ACCTBAL
    FROM
        customer
    WHERE
        SUBSTR(C_PHONE , 1 , 2) IN
        ('20', '40', '22', '30', '39', '42', '21')
    AND C_ACCTBAL > (
            SELECT
                AVG(C_ACCTBAL)
            FROM
                customer
            WHERE
                C_ACCTBAL > 0.00
            AND SUBSTR(C_PHONE , 1 , 2) IN
            ('20', '40', '22', '30', '39', '42', '21')
        )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            orders
        WHERE
            O_CUSTKEY = C_CUSTKEY
        )
    ) AS CUSTSALE
GROUP BY
    CNTRYCODE
ORDER BY
    CNTRYCODE
"""

cursor.execute(query)
rows = cursor.fetchall()

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as f:
    writer = csv.writer(f)
    # Write header
    writer.writerow(['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL'])
    # Write data
    for row in rows:
        writer.writerow(row)

# Cleanup
cursor.close()
cnx.close()
