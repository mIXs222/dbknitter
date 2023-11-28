import pymysql
import csv

# MySQL connection setup
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

try:
    with mysql_connection.cursor() as cursor:
        # SQL Query
        query = """
        SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
        FROM lineitem
        INNER JOIN part ON part.P_PARTKEY = lineitem.L_PARTKEY
        WHERE (part.P_BRAND = 'Brand#12'
            AND part.P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND lineitem.L_QUANTITY BETWEEN 1 AND 11
            AND part.P_SIZE BETWEEN 1 AND 5
            AND lineitem.L_SHIPMODE IN ('AIR', 'AIR REG')
            AND lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
        OR   (part.P_BRAND = 'Brand#23'
            AND part.P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            AND lineitem.L_QUANTITY BETWEEN 10 AND 20
            AND part.P_SIZE BETWEEN 1 AND 10
            AND lineitem.L_SHIPMODE IN ('AIR', 'AIR REG')
            AND lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
        OR   (part.P_BRAND = 'Brand#34'
            AND part.P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            AND lineitem.L_QUANTITY BETWEEN 20 AND 30
            AND part.P_SIZE BETWEEN 1 AND 15
            AND lineitem.L_SHIPMODE IN ('AIR', 'AIR REG')
            AND lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
        """

        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            with open('query_output.csv', 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(['total_revenue'])
                csvwriter.writerow([result[0]])
finally:
    mysql_connection.close()
