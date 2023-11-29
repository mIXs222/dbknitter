import pymysql
import csv
from datetime import datetime

# Establish connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        sql_query = """
        SELECT
          c.C_NAME AS CUST_NATION,
          YEAR(o.O_ORDERDATE) AS L_YEAR,
          SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
          s.S_NAME AS SUPP_NATION
        FROM
          customer AS c
          JOIN orders AS o ON c.C_CUSTKEY = o.O_CUSTKEY
          JOIN lineitem AS l ON o.O_ORDERKEY = l.L_ORDERKEY
          JOIN supplier AS s ON l.L_SUPPKEY = s.S_SUPPKEY
          JOIN nation AS n1 ON s.S_NATIONKEY = n1.N_NATIONKEY
          JOIN nation AS n2 ON c.C_NATIONKEY = n2.N_NATIONKEY
        WHERE
          n1.N_NAME IN ('INDIA', 'JAPAN')
          AND n2.N_NAME IN ('INDIA', 'JAPAN')
          AND n1.N_NAME != n2.N_NAME
          AND o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
        GROUP BY
          CUST_NATION, L_YEAR, SUPP_NATION
        ORDER BY
          SUPP_NATION, CUST_NATION, L_YEAR;
        """

        cursor.execute(sql_query)
        result = cursor.fetchall()

        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION'])
            for row in result:
                writer.writerow(row)
finally:
    connection.close()
