import csv
import mysql.connector
from pymongo import MongoClient

#Define the MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']

#Define the MySQL connection
cnx = mysql.connector.connect(user='your_mysql_user', password='your_mysql_password',
                              host='your_mysql_host',
                              database='tpch')

#Define the MySQL query
mysql_query = """
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
    CNTRYCODE"""

#Execute the MySQL query
cursor = cnx.cursor()
cursor.execute(mysql_query)

#Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([i[0] for i in cursor.description])  # write headers
    writer.writerows(cursor.fetchall())                  # write data

cursor.close()
cnx.close()
