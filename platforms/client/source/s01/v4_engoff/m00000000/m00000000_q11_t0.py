import pymysql
import csv

# Connection information
conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# Establish connection to the MySQL database
connection = pymysql.connect(**conn_info)

# SQL Query
query = """
SELECT P.PS_PARTKEY, SUM(P.PS_SUPPLYCOST * P.PS_AVAILQTY) AS TOTAL_VALUE
FROM partsupp P
JOIN supplier S ON P.PS_SUPPKEY = S.S_SUPPKEY
JOIN nation N ON S.S_NATIONKEY = N.N_NATIONKEY
WHERE N.N_NAME = 'GERMANY'
GROUP BY P.PS_PARTKEY
HAVING SUM(P.PS_SUPPLYCOST * P.PS_AVAILQTY) > (
    SELECT SUM(P.PS_SUPPLYCOST * P.PS_AVAILQTY) / 10 FROM partsupp P
    JOIN supplier S ON P.PS_SUPPKEY = S.S_SUPPKEY
    JOIN nation N ON S.S_NATIONKEY = N.N_NATIONKEY
    WHERE N.N_NAME = 'GERMANY'
)
ORDER BY TOTAL_VALUE DESC;
"""

try:
    with connection.cursor() as cursor:
        cursor.execute(query)
        result_set = cursor.fetchall()

        # Write the query output to a CSV file
        with open('query_output.csv', mode='w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['PS_PARTKEY', 'TOTAL_VALUE'])
            for row in result_set:
                csv_writer.writerow(row)
finally:
    connection.close()
