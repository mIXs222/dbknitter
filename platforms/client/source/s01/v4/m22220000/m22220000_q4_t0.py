import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # Execute the SQL query
        sql_query = """
            SELECT
                O_ORDERPRIORITY,
                COUNT(*) AS ORDER_COUNT
            FROM
                orders
            WHERE
                O_ORDERDATE >= '1993-07-01'
                AND O_ORDERDATE < '1993-10-01'
                AND EXISTS (
                    SELECT
                        *
                    FROM
                        lineitem
                    WHERE
                        L_ORDERKEY = O_ORDERKEY
                        AND L_COMMITDATE < L_RECEIPTDATE
                )
            GROUP BY
                O_ORDERPRIORITY
            ORDER BY
                O_ORDERPRIORITY
        """
        cursor.execute(sql_query)

        # Fetch the result
        result = cursor.fetchall()

        # Write the result to a CSV file
        with open('query_output.csv', 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])  # Write header
            for row in result:
                csv_writer.writerow(row)

finally:
    # Close the database connection
    connection.close()
