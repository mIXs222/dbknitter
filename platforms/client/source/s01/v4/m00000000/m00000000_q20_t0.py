import pymysql
import csv

# Establish connection to MySQL
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor  # Using the default cursor, not DictCursor
)

try:
    with connection.cursor() as cursor:
        # Execute the given query
        cursor.execute("""
            SELECT
                S_NAME,
                S_ADDRESS
            FROM
                supplier,
                nation
            WHERE
                S_SUPPKEY IN (
                SELECT
                    PS_SUPPKEY
                FROM
                    partsupp
                WHERE
                    PS_PARTKEY IN (
                        SELECT
                            P_PARTKEY
                        FROM
                            part
                        WHERE
                            P_NAME LIKE 'forest%'
                    )
                    AND PS_AVAILQTY > (
                        SELECT
                            0.5 * SUM(L_QUANTITY)
                        FROM
                            lineitem
                        WHERE
                            L_PARTKEY = PS_PARTKEY
                            AND L_SUPPKEY = PS_SUPPKEY
                            AND L_SHIPDATE >= '1994-01-01'
                            AND L_SHIPDATE < '1995-01-01'
                        )
                )
                AND S_NATIONKEY = N_NATIONKEY
                AND N_NAME = 'CANADA'
            ORDER BY
                S_NAME
        """)

        # Fetch all results
        results = cursor.fetchall()
        
        # Write query results to CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['S_NAME', 'S_ADDRESS'])  # Header
            
            for row in results:
                writer.writerow(row)

finally:
    connection.close()
