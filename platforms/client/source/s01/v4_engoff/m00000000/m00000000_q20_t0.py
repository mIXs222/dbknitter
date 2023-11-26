import pymysql
import csv
from datetime import datetime

# Establish a connection to the MySQL database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

try:
    with connection.cursor() as cursor:
        query = """
        SELECT s.S_NAME, s.S_ADDRESS
        FROM supplier s
        JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
        JOIN part p ON ps.PS_PARTKEY = p.P_PARTKEY
        JOIN lineitem l ON l.L_SUPPKEY = s.S_SUPPKEY AND l.L_PARTKEY = p.P_PARTKEY
        JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
        WHERE n.N_NAME = 'CANADA'
        AND p.P_NAME LIKE 'forest%'
        AND l.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
        GROUP BY s.S_SUPPKEY
        HAVING SUM(l.L_QUANTITY) > 0.5 * (
            SELECT SUM(L_QUANTITY)
            FROM lineitem
            WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
            AND L_PARTKEY IN (
                SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'
            )
        )
        ORDER BY s.S_NAME;
        """
        cursor.execute(query)
        result = cursor.fetchall()

        # Write the output to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['S_NAME', 'S_ADDRESS'])
            for row in result:
                writer.writerow(row)
finally:
    connection.close()
