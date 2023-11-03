import pymysql.cursors
import csv

# Connect to the database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # Execute the SQL query
        sql = """
            SELECT
                L_ORDERKEY,
                SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
                O_ORDERDATE,
                O_SHIPPRIORITY
            FROM
                CUSTOMER,
                ORDERS,
                LINEITEM
            WHERE
                C_MKTSEGMENT = 'BUILDING'
                AND C_CUSTKEY = O_CUSTKEY
                AND L_ORDERKEY = O_ORDERKEY
                AND O_ORDERDATE < '1995-03-15'
                AND L_SHIPDATE > '1995-03-15'
            GROUP BY
                L_ORDERKEY,
                O_ORDERDATE,
                O_SHIPPRIORITY
            ORDER BY
                REVENUE DESC,
                O_ORDERDATE
        """
        cursor.execute(sql)

        # Write the result to a CSV file
        with open('query_output.csv', 'w', newline='') as file:
            fieldnames = ['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            for row in cursor:
                writer.writerow(row)

finally:
    connection.close()
