import pymysql
import csv

# Define MySQL connection parameters
MYSQL_DATABASE = 'tpch'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'my-secret-pw'
MYSQL_HOST = 'mysql'

# Connect to the MySQL database
connection = pymysql.connect(host=MYSQL_HOST,
                             user=MYSQL_USER,
                             password=MYSQL_PASSWORD,
                             database=MYSQL_DATABASE,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        # Construct the query
        query = '''
        SELECT p.P_BRAND, p.P_TYPE, p.P_SIZE, COUNT(DISTINCT ps.PS_SUPPKEY) AS SUPPLIER_COUNT
        FROM part p
        JOIN partsupp ps ON p.P_PARTKEY = ps.PS_PARTKEY
        JOIN supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY
        WHERE p.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
          AND p.P_BRAND <> 'Brand#45'
          AND p.P_TYPE NOT LIKE 'MEDIUM POLISHED%'
          AND s.S_COMMENT NOT LIKE '%Customer%Complaints%'
        GROUP BY p.P_BRAND, p.P_TYPE, p.P_SIZE
        ORDER BY SUPPLIER_COUNT DESC, p.P_BRAND ASC, p.P_TYPE ASC, p.P_SIZE;
        '''
        # Execute the query
        cursor.execute(query)

        # Write the results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_COUNT'])  # write header
            for row in cursor.fetchall():
                csv_writer.writerow(row)
finally:
    # Close the connection
    connection.close()
