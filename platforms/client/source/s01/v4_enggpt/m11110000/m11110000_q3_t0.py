import pymysql
import csv

# Database connection parameters
db_params = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Connect to MySQL
connection = pymysql.connect(**db_params)
try:
    with connection.cursor() as cursor:
        sql_query = """
        SELECT
            O_ORDERKEY,
            O_ORDERDATE,
            O_SHIPPRIORITY,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
        FROM
            customer
        JOIN orders ON O_CUSTKEY = C_CUSTKEY
        JOIN lineitem ON L_ORDERKEY = O_ORDERKEY
        WHERE
            C_MKTSEGMENT = 'BUILDING'
            AND O_ORDERDATE < '1995-03-15'
            AND L_SHIPDATE > '1995-03-15'
        GROUP BY
            O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
        ORDER BY
            REVENUE DESC, O_ORDERDATE
        """
        cursor.execute(sql_query)
        result = cursor.fetchall()

        # Write results to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Writing headers
            csvwriter.writerow(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'REVENUE'])
            # Writing data
            for row in result:
                csvwriter.writerow(row)
finally:
    connection.close()
