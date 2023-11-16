import pymysql
import csv

# MySQL connection parameters
MYSQL_DB = 'tpch'
MYSQL_USER = 'root'
MYSQL_PASS = 'my-secret-pw'
MYSQL_HOST = 'mysql'

# Connect to MySQL
mysql_conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASS, db=MYSQL_DB)
mysql_cursor = mysql_conn.cursor()

# Query
mysql_query = """
SELECT
    C_NAME,
    C.C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE,
    SUM(L_QUANTITY) as total_quantity
FROM
    customer C
JOIN
    orders O ON C.C_CUSTKEY = O.O_CUSTKEY
JOIN
    lineitem L ON O.O_ORDERKEY = L.L_ORDERKEY
WHERE
    O.O_ORDERKEY IN (
        SELECT L_ORDERKEY
        FROM lineitem
        GROUP BY L_ORDERKEY
        HAVING SUM(L_QUANTITY) > 300
    )
GROUP BY
    C_NAME,
    C.C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE
ORDER BY
    O_TOTALPRICE DESC,
    O_ORDERDATE
"""

# Execute query in MySQL
mysql_cursor.execute(mysql_query)

# Fetch all results
results = mysql_cursor.fetchall()

# Writing results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Writing header
    csvwriter.writerow([i[0] for i in mysql_cursor.description])
    # Writing data rows
    for row in results:
        csvwriter.writerow(row)

# Close cursor and connection
mysql_cursor.close()
mysql_conn.close()
