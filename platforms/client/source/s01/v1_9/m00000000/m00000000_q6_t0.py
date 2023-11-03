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
        # SQL Query
        sql = """
        SELECT
            SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
        FROM
            LINEITEM
        WHERE
            L_SHIPDATE >= '1994-01-01'
            AND L_SHIPDATE < '1995-01-01'
            AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
            AND L_QUANTITY < 24
        """

        cursor.execute(sql)
        result = cursor.fetchone()

        with open('query_output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(result.keys())
            writer.writerow(result.values())
        
finally:
    connection.close()
