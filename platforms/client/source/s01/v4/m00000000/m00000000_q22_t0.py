# Python code to execute the SQL query
import pymysql
import csv

# Database connection parameters
db_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# SQL query from the user
sql_query = """
SELECT
    CNTRYCODE,
    COUNT(*) AS NUMCUST,
    SUM(C_ACCTBAL) AS TOTACCTBAL
FROM
    (
    SELECT
        SUBSTR(C_PHONE, 1 , 2) AS CNTRYCODE,
        C_ACCTBAL
    FROM
        customer
    WHERE
        SUBSTR(C_PHONE , 1 , 2) IN
        ('20', '40', '22', '30', '39', '42', '21')
    AND C_ACCTBAL > (
            SELECT
                AVG(C_ACCTBAL)
            FROM
                customer
            WHERE
                C_ACCTBAL > 0.00
            AND SUBSTR(C_PHONE , 1 , 2) IN
            ('20', '40', '22', '30', '39', '42', '21')
    )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            orders
        WHERE
            O_CUSTKEY = C_CUSTKEY
        )
    ) AS CUSTSALE
GROUP BY
    CNTRYCODE
ORDER BY
    CNTRYCODE
"""

# Connect to the MySQL database
connection = pymysql.connect(**db_params)
try:
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        results = cursor.fetchall()
        
        # Write the query results to query_output.csv file
        with open('query_output.csv', mode='w', newline='') as f:
            csv_writer = csv.writer(f)
            # Write headers
            csv_writer.writerow([i[0] for i in cursor.description])
            # Write data rows
            for row in results:
                csv_writer.writerow(row)
finally:
    connection.close()
