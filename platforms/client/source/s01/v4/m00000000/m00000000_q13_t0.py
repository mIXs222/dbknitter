import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Execute the query
query = """
SELECT
    C_COUNT,
    COUNT(*) AS CUSTDIST
FROM
    (
    SELECT
        C_CUSTKEY,
        COUNT(O_ORDERKEY) AS C_COUNT
    FROM
        customer LEFT OUTER JOIN orders ON
        C_CUSTKEY = O_CUSTKEY
        AND O_COMMENT NOT LIKE '%pending%deposits%'
    GROUP BY
        C_CUSTKEY
    ) as C_ORDERS
GROUP BY
    C_COUNT
ORDER BY
    CUSTDIST DESC,
    C_COUNT DESC
"""

try:
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        # Write query output to csv file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the headers
            csvwriter.writerow(['C_COUNT', 'CUSTDIST'])
            # Write data rows
            csvwriter.writerows(result)
finally:
    connection.close()
