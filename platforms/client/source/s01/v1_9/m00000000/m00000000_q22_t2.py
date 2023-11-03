import mysql.connector
import csv

#establishing the connection
conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Executing an MYSQL function using the execute() method
cursor.execute("""
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
""")

result = cursor.fetchall()

header = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']

with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(result)

#close the cursor and connection  
cursor.close()
conn.close()
