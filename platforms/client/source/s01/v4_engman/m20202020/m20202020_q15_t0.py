import pymysql
import csv

# Define the connection parameters
mysql_conn_details = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch",
}

# Establish a connection to the MySQL database
mysql_conn = pymysql.connect(
    host=mysql_conn_details["host"],
    user=mysql_conn_details["user"],
    password=mysql_conn_details["password"],
    database=mysql_conn_details["database"]
)

# Define the SQL query to execute
query = """
SELECT
    supplier.S_SUPPKEY,
    supplier.S_NAME,
    supplier.S_ADDRESS,
    supplier.S_PHONE,
    SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS TOTAL_REVENUE
FROM
    supplier,
    lineitem
WHERE
    supplier.S_SUPPKEY = lineitem.L_SUPPKEY
    AND lineitem.L_SHIPDATE >= '1996-01-01'
    AND lineitem.L_SHIPDATE < '1996-04-01'
GROUP BY
    supplier.S_SUPPKEY
HAVING
    SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) = (
        SELECT MAX(total_revenue)
        FROM (
            SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
            FROM lineitem
            WHERE L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE < '1996-04-01'
            GROUP BY L_SUPPKEY
        ) AS rev
    )
ORDER BY
    supplier.S_SUPPKEY;
"""

# Execute the SQL query and fetch the results
with mysql_conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

# Write the results to a CSV file
with open("query_output.csv", "w", newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
    for row in results:
        writer.writerow(row)

# Close the connection
mysql_conn.close()
