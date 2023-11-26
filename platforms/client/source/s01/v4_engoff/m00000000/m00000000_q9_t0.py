import pymysql
import csv
from datetime import datetime

# Define the connection parameters
mysql_conn_params = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# SQL Query
sql_query = """
SELECT
    n.N_NAME AS nation,
    EXTRACT(YEAR FROM o.O_ORDERDATE) AS o_year,
    SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) AS profit
FROM
    nation n,
    orders o,
    lineitem l,
    partsupp ps,
    supplier s,
    part p
WHERE
    p.P_NAME LIKE '%dim%'
    AND s.S_SUPPKEY = l.L_SUPPKEY
    AND ps.PS_SUPPKEY = l.L_SUPPKEY
    AND ps.PS_PARTKEY = l.L_PARTKEY
    AND n.N_NATIONKEY = s.S_NATIONKEY
    AND l.L_ORDERKEY = o.O_ORDERKEY
    AND p.P_PARTKEY = l.L_PARTKEY
GROUP BY
    nation,
    o_year
ORDER BY
    nation ASC,
    o_year DESC;
"""

# Connect to MySQL database, execute query, and write results to CSV
try:
    # Establish a database connection
    mysql_conn = pymysql.connect(**mysql_conn_params)
    cursor = mysql_conn.cursor()

    # Execute the SQL query
    cursor.execute(sql_query)

    # Write the output to CSV file
    with open('query_output.csv', mode='w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write headers
        csv_writer.writerow(['nation', 'o_year', 'profit'])
        # Write data rows
        for row in cursor.fetchall():
            csv_writer.writerow(row)

    print("Query results successfully written to 'query_output.csv'")
    
except pymysql.MySQLError as e:
    print(f"Error connecting to MySQL Platform: {e}")
finally:
    # Close the cursor and the connection
    cursor.close()
    mysql_conn.close()
