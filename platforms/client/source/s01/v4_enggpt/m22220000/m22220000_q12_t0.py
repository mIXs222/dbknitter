import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        query = """
        SELECT 
            L_SHIPMODE,
            SUM(CASE WHEN O_ORDERPRIORITY = '1-URGENT' OR O_ORDERPRIORITY = '2-HIGH' THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
            SUM(CASE WHEN O_ORDERPRIORITY <> '1-URGENT' AND O_ORDERPRIORITY <> '2-HIGH' THEN 1 ELSE 0 END) AS LOW_LINE_COUNT
        FROM 
            orders INNER JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
        WHERE 
            L_SHIPMODE IN ('MAIL', 'SHIP') AND
            L_COMMITDATE < L_RECEIPTDATE AND 
            L_SHIPDATE < L_COMMITDATE AND 
            L_RECEIPTDATE BETWEEN '1994-01-01' AND '1994-12-31'
        GROUP BY 
            L_SHIPMODE
        ORDER BY 
            L_SHIPMODE;
        """
        
        cursor.execute(query)
        result = cursor.fetchall()

        # Open or create a CSV file and write the headers and data
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the header
            csvwriter.writerow(['SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
            # Write the data
            for row in result:
                csvwriter.writerow([row[0], row[1], row[2]])

finally:
    connection.close()
