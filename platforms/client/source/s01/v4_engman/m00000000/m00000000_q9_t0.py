# file: run_query.py

import pymysql
import csv
from datetime import datetime

# Query to be executed
QUERY = """
SELECT
    n.N_NAME as nation,
    YEAR(o.O_ORDERDATE) as o_year,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) - ps.PS_SUPPLYCOST * l.L_QUANTITY) as profit
FROM
    nation n
INNER JOIN
    supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
INNER JOIN
    partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
INNER JOIN
    lineitem l ON ps.PS_PARTKEY = l.L_PARTKEY AND ps.PS_SUPPKEY = l.L_SUPPKEY
INNER JOIN
    orders o ON l.L_ORDERKEY = o.O_ORDERKEY
INNER JOIN
    part p ON p.P_PARTKEY = l.L_PARTKEY
WHERE
    p.P_NAME like '%dim%'
GROUP BY
    nation, o_year
ORDER BY
    nation ASC, o_year DESC;
"""

# Function to run the query and write results
def run_query_and_write_output():
    # Connect to the MySQL server
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(QUERY)

            # Fetch all the records
            result = cursor.fetchall()

            # Write the result to query_output.csv
            with open('query_output.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['nation', 'year', 'profit'])
                for row in result:
                    writer.writerow(row)
    finally:
        connection.close()

if __name__ == '__main__':
    run_query_and_write_output()
