import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    query = """
    SELECT
        p.P_BRAND,
        p.P_TYPE,
        p.P_SIZE,
        COUNT(DISTINCT ps.PS_SUPPKEY) AS SUPPLIER_CNT
    FROM
        part p
    INNER JOIN partsupp ps ON p.P_PARTKEY = ps.PS_PARTKEY
    WHERE
        p.P_BRAND <> 'Brand#45'
        AND p.P_TYPE NOT LIKE 'MEDIUM POLISHED%'
        AND p.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
        AND NOT EXISTS (
            SELECT 1 
            FROM supplier s 
            WHERE s.S_SUPPKEY = ps.PS_SUPPKEY 
            AND s.S_COMMENT LIKE '%Customer Complaints%'
        )
    GROUP BY
        p.P_BRAND,
        p.P_TYPE,
        p.P_SIZE
    ORDER BY
        SUPPLIER_CNT DESC,
        p.P_BRAND ASC,
        p.P_TYPE ASC,
        p.P_SIZE ASC
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

        # Writing result to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])
            for row in result:
                writer.writerow(row)

finally:
    connection.close()
