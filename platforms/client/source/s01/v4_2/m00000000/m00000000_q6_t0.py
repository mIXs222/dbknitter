import pymysql
import csv 

connection = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    db='tpch')

try:
    with connection.cursor() as cursor:
        sql_query = """SELECT
                        SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
                        FROM
                            lineitem
                        WHERE
                            L_SHIPDATE >= '1994-01-01'
                            AND L_SHIPDATE < '1995-01-01'
                            AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
                            AND L_QUANTITY < 24"""
        cursor.execute(sql_query)
        result = cursor.fetchall()

        with open('query_output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['REVENUE'])
            for row in result:
                writer.writerow(row)

finally:
    connection.close()

