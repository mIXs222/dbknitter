import pymysql
import csv

# Define connection information
connection_info = {
    "tpch": {
        "user": "root",
        "password": "my-secret-pw",
        "host": "mysql",
        "database": "tpch",
    }
}

# Establish the MySQL connection
connection = pymysql.connect(
    host=connection_info["tpch"]["host"],
    user=connection_info["tpch"]["user"],
    password=connection_info["tpch"]["password"],
    db=connection_info["tpch"]["database"],
)

try:
    with connection.cursor() as cursor:
        sql = """
        SELECT
            L_SHIPMODE,
            SUM(CASE
                WHEN O_ORDERPRIORITY = '1-URGENT'
                OR O_ORDERPRIORITY = '2-HIGH'
                THEN 1
                ELSE 0
            END) AS HIGH_LINE_COUNT,
            SUM(CASE
                WHEN O_ORDERPRIORITY <> '1-URGENT'
                AND O_ORDERPRIORITY <> '2-HIGH'
                THEN 1
                ELSE 0
            END) AS LOW_LINE_COUNT
        FROM
            orders,
            lineitem
        WHERE
            O_ORDERKEY = L_ORDERKEY
            AND L_SHIPMODE IN ('MAIL', 'SHIP')
            AND L_COMMITDATE < L_RECEIPTDATE
            AND L_SHIPDATE < L_COMMITDATE
            AND L_RECEIPTDATE >= '1994-01-01'
            AND L_RECEIPTDATE < '1995-01-01'
        GROUP BY
            L_SHIPMODE
        ORDER BY
            L_SHIPMODE
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        
        # Write result to CSV
        with open('query_output.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT']) # Headers
            for row in rows:
                writer.writerow(row)
finally:
    connection.close()
