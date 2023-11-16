import pymysql
import csv

# MySQL connection setup
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)
mysql_cursor = mysql_conn.cursor()

# The query to get the data
query = """
SELECT
    PS_PARTKEY,
    SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE
FROM
    partsupp
JOIN
    supplier ON PS_SUPPKEY = S_SUPPKEY
JOIN
    nation ON S_NATIONKEY = N_NATIONKEY
WHERE
    N_NAME = 'GERMANY'
GROUP BY
    PS_PARTKEY HAVING
    SUM(PS_SUPPLYCOST * PS_AVAILQTY) >
    (
    SELECT
        SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000
    FROM
        partsupp
    JOIN
        supplier ON PS_SUPPKEY = S_SUPPKEY
    JOIN
        nation ON S_NATIONKEY = N_NATIONKEY
    WHERE
        N_NAME = 'GERMANY'
    )
ORDER BY
    VALUE DESC
"""

# Execute the query on MySQL
mysql_cursor.execute(query)
results = mysql_cursor.fetchall()

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['PS_PARTKEY', 'VALUE'])  # Writing the headers
    for row in results:
        csv_writer.writerow(row)

# Close the cursor and connection
mysql_cursor.close()
mysql_conn.close()
