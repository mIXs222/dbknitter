import pymysql
import csv

# Database connection
connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

try:
    with connection.cursor() as cursor:
        # The actual query executed on the MySQL database
        sql_query = """
        SELECT
            S_NAME,
            COUNT(*) AS NUMWAIT
        FROM
            supplier,
            lineitem AS L1,
            orders,
            nation
        WHERE
            S_SUPPKEY = L1.L_SUPPKEY
            AND O_ORDERKEY = L1.L_ORDERKEY
            AND O_ORDERSTATUS = 'F'
            AND L1.L_RECEIPTDATE > L1.L_COMMITDATE
            AND EXISTS (
                SELECT
                    *
                FROM
                    lineitem AS L2
                WHERE
                    L2.L_ORDERKEY = L1.L_ORDERKEY
                    AND L2.L_SUPPKEY <> L1.L_SUPPKEY
            )
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    lineitem AS L3
                WHERE
                    L3.L_ORDERKEY = L1.L_ORDERKEY
                    AND L3.L_SUPPKEY <> L1.L_SUPPKEY
                    AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
                )
            AND S_NATIONKEY = N_NATIONKEY
            AND N_NAME = 'SAUDI ARABIA'
        GROUP BY
            S_NAME
        ORDER BY
            NUMWAIT DESC,
            S_NAME
        """
        
        cursor.execute(sql_query)
        result = cursor.fetchall()

        # Write query's output to a csv file
        with open('query_output.csv', mode='w', newline='') as file:
            csv_writer = csv.writer(file)
            # Write header
            csv_writer.writerow([i[0] for i in cursor.description])
            # Write data
            for row in result:
                csv_writer.writerow(row)

finally:
    # Close the connection
    connection.close()
