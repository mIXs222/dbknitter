import pymysql
import csv

# Connection parameters
connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.Cursor,
}

# Query
query = """
SELECT
    c.C_NAME,
    c.C_ADDRESS,
    n.N_NAME,
    c.C_PHONE,
    c.C_ACCTBAL,
    c.C_COMMENT,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue_lost
FROM
    customer c
JOIN
    orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN
    nation n ON c.C_NATIONKEY = n.N_NATIONKEY
WHERE
    l.L_RETURNFLAG = 'R'
    AND o.O_ORDERDATE >= '1993-10-01'
    AND o.O_ORDERDATE < '1994-01-01'
GROUP BY
    c.C_CUSTKEY, c.C_NAME, c.C_ADDRESS, n.N_NAME, c.C_PHONE, c.C_ACCTBAL, c.C_COMMENT
ORDER BY
    revenue_lost DESC,
    c.C_CUSTKEY,
    c.C_NAME,
    c.C_ACCTBAL
"""

# Connect to the MySQL database
connection = pymysql.connect(**connection_params)

try:
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    # Write the query result to a CSV file
    with open('query_output.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # Writing the header
        writer.writerow(['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'revenue_lost'])
        # Writing the data rows
        for row in result:
            writer.writerow(row)
finally:
    connection.close()
