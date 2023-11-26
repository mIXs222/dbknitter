import csv
import pymysql

def main():
    # Connect to the MySQL database
    mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

    # SQL query to execute
    query = """
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

    try:
        with mysql_conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

            # Save the results to query_output.csv
            with open('query_output.csv', 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])  # column headers
                for row in results:
                    csvwriter.writerow(row)
    finally:
        mysql_conn.close()

if __name__ == '__main__':
    main()
