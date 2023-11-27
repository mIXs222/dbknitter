import pymysql
import csv
from datetime import datetime

# Connection information for the MySQL database
mysql_connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# Connect to the MySQL Database
mysql_conn = pymysql.connect(
    host=mysql_connection_info['host'],
    user=mysql_connection_info['user'],
    password=mysql_connection_info['password'],
    database=mysql_connection_info['database'],
)

try:
    # Create a cursor for MySQL Connection
    mysql_cursor = mysql_conn.cursor()

    # Define the SQL Query
    mysql_query = """
    SELECT
        n.N_NAME,
        EXTRACT(YEAR FROM o.O_ORDERDATE) AS year,
        SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
    FROM
        part p
        JOIN lineitem l ON p.P_PARTKEY = l.L_PARTKEY
        JOIN partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
        JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
        JOIN supplier s ON s.S_SUPPKEY = l.L_SUPPKEY
        JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
    WHERE
        p.P_NAME LIKE '%dim%'
    GROUP BY
        n.N_NAME,
        year
    ORDER BY
        n.N_NAME ASC,
        year DESC
    """

    # Execute the SQL Query against MySQL
    mysql_cursor.execute(mysql_query)

    # Fetch all the results
    mysql_results = mysql_cursor.fetchall()

    # Writing to CSV
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["nation", "year", "profit"])
        for row in mysql_results:
            writer.writerow(row)

finally:
    # Close the MySQL Cursor and Connection
    if mysql_conn:
        mysql_conn.close()
