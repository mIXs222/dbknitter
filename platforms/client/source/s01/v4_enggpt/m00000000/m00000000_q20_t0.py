import pymysql
import csv


# Connect to the MySQL server
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
)

try:
    with connection.cursor() as cursor:
        # Construct SQL query as per the requirement
        sql = """
        SELECT s.S_NAME, s.S_ADDRESS
        FROM supplier s
        INNER JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
        WHERE n.N_NAME = 'CANADA' 
        AND s.S_SUPPKEY IN (
            SELECT DISTINCT ps.PS_SUPPKEY
            FROM partsupp ps 
            INNER JOIN part p ON p.P_PARTKEY = ps.PS_PARTKEY
            WHERE p.P_NAME LIKE 'forest%'
            AND ps.PS_AVAILQTY > (
                SELECT 0.5 * SUM(l.L_QUANTITY)
                FROM lineitem l
                WHERE l.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01' 
                AND l.L_PARTKEY = ps.PS_PARTKEY 
                AND l.L_SUPPKEY = ps.PS_SUPPKEY
            )
        )
        ORDER BY s.S_NAME ASC;
        """
        
        cursor.execute(sql)
        
        # fetchall() returns all records from the query
        results = cursor.fetchall()
        
        # Write the results to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['S_NAME', 'S_ADDRESS'])  # Column headers
            for row in results:
                writer.writerow(row)

finally:
    # Close the MySQL connection
    connection.close()
