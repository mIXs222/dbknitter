import mysql.connector
import csv

# Define your database configuration
db_config = {
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql",
    "database": "tpch",
}

# Connect to MySQL server
cnx = mysql.connector.connect(**db_config)
cursor = cnx.cursor()

# Define your SQL query
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
        AND O_COMMENT NOT LIKE '%%pending%%deposits%%'
    GROUP BY
        C_CUSTKEY
    )   C_ORDERS
GROUP BY
    C_COUNT
ORDER BY
    CUSTDIST DESC,
    C_COUNT DESC
"""

# Execute your SQL query
cursor.execute(sql_query)

# Open your CSV file and write the data into it
with open("query_output.csv", "w") as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow([i[0] for i in cursor.description])  # write headers
    csv_writer.writerows(cursor.fetchall())

# Don't forget to close the database connection
cnx.close()
