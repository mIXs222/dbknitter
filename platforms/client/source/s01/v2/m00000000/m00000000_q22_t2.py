import mysql.connector
import csv

database_config = {
  'user': 'root',
  'password': 'my-secret-pw',
  'host': 'mysql',
  'database': 'tpch',
}

db = mysql.connector.connect(**database_config)

cur = db.cursor()

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

cur.execute(query)

rows = cur.fetchall()

header = [i[0] for i in cur.description]

with open('query_output.csv', 'w', newline='') as f_output:
    csv_output = csv.writer(f_output)
    csv_output.writerow(header)
    csv_output.writerows(rows)

db.close()
