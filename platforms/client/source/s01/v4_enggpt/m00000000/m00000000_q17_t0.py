import pymysql
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

try:
    with mysql_conn.cursor() as cursor:
        query = """
        SELECT 
            YEAR(L_SHIPDATE) AS year,
            SUM(L_EXTENDEDPRICE) / 7.0 AS avg_yearly_extended_price
        FROM
            lineitem
        INNER JOIN
            part ON L_PARTKEY = P_PARTKEY
        WHERE
            P_BRAND = 'Brand#23'
            AND P_CONTAINER = 'MED BAG'
            AND L_QUANTITY <
                (SELECT 
                    0.2 * AVG(L2.L_QUANTITY)
                FROM
                    lineitem L2
                WHERE
                    L2.L_PARTKEY = part.P_PARTKEY)
        GROUP BY
            YEAR(L_SHIPDATE);
        """
        
        cursor.execute(query)
        results = cursor.fetchall()

        with open('query_output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['year', 'avg_yearly_extended_price'])
            for row in results:
                writer.writerow(row)

finally:
    mysql_conn.close()
