import pymysql
import csv

# MySQL connection
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')
try:
    with connection.cursor() as cursor:
        # SQL query
        sql_query = """
        SELECT P_BRAND, P_TYPE, P_SIZE, COUNT(DISTINCT S_SUPPKEY) as SUPPLIER_COUNT
        FROM part, partsupp, supplier
        WHERE
            P_PARTKEY = PS_PARTKEY
            AND S_SUPPKEY = PS_SUPPKEY
            AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
            AND P_TYPE NOT LIKE '%MEDIUM POLISHED%'
            AND P_BRAND NOT LIKE 'Brand#45'
            AND S_COMMENT NOT LIKE '%Customer%Complaints%'
        GROUP BY P_BRAND, P_TYPE, P_SIZE
        ORDER BY SUPPLIER_COUNT DESC, P_BRAND ASC, P_TYPE ASC, P_SIZE ASC;
        """
        cursor.execute(sql_query)
        result = cursor.fetchall()
    # Write query output to csv file
    with open('query_output.csv', mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        # Write the header
        csv_writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_COUNT'])
        # Write the data rows
        for row in result:
            csv_writer.writerow(row)
finally:
    connection.close()
