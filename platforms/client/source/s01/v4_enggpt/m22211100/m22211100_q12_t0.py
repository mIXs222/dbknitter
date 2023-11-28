# Python Code: query.py

import pymysql
import csv
from datetime import datetime

def main():
    # Connect to MySQL database
    connection = pymysql.connect(host="mysql",
                                 user="root",
                                 password="my-secret-pw",
                                 db="tpch")

    try:
        with connection.cursor() as cursor:
            # Execute the query
            sql_query = """
            SELECT L_SHIPMODE, 
                SUM(CASE WHEN O_ORDERPRIORITY IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
                SUM(CASE WHEN O_ORDERPRIORITY NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS LOW_LINE_COUNT
            FROM lineitem
            INNER JOIN orders ON L_ORDERKEY = O_ORDERKEY
            WHERE (L_SHIPMODE IN ('MAIL', 'SHIP')) 
                AND (L_COMMITDATE < L_RECEIPTDATE) AND (L_SHIPDATE < L_COMMITDATE)
                AND (L_RECEIPTDATE BETWEEN '1994-01-01' AND '1994-12-31')
            GROUP BY L_SHIPMODE
            ORDER BY L_SHIPMODE ASC;
            """
            cursor.execute(sql_query)
            results = cursor.fetchall()

            # Write query results to CSV file
            with open('query_output.csv', 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                # Write the header
                csvwriter.writerow(['SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
                # Write the data
                for row in results:
                    csvwriter.writerow(row)

    finally:
        connection.close()

if __name__ == "__main__":
    main()
