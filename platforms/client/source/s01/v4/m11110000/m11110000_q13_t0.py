import pymysql
import csv

# Connection details
db_config = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch",
}

# Connect to the MySQL database
connection = pymysql.connect(**db_config)

# SQL query
sql_query = """
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
    ) C_ORDERS
GROUP BY
    C_COUNT
ORDER BY
    CUSTDIST DESC,
    C_COUNT DESC
"""

# Execute the query
try:
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        results = cursor.fetchall()

        # Write query output to csv file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write header
            csv_writer.writerow(['C_COUNT', 'CUSTDIST'])
            # Write data
            for row in results:
                csv_writer.writerow(row)
finally:
    connection.close()
