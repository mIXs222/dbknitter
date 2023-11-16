import pymysql
import csv

# Define the connection parameters
connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connect to the MySQL database
connection = pymysql.connect(**connection_params)

try:
    with connection.cursor() as cursor:
        # Execute the query
        query = """
            SELECT
                P_BRAND,
                P_TYPE,
                P_SIZE,
                COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
            FROM
                part
            JOIN
                partsupp ON P_PARTKEY = PS_PARTKEY
            WHERE
                P_BRAND <> 'Brand#45'
                AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
                AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
                AND PS_SUPPKEY NOT IN (
                    SELECT
                        S_SUPPKEY
                    FROM
                        supplier
                    WHERE
                        S_COMMENT LIKE '%Customer%Complaints%'
                )
            GROUP BY
                P_BRAND,
                P_TYPE,
                P_SIZE
            ORDER BY
                SUPPLIER_CNT DESC,
                P_BRAND,
                P_TYPE,
                P_SIZE
        """

        cursor.execute(query)
        # Fetch all the rows
        rows = cursor.fetchall()

        # Write results to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the headers
            csvwriter.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])
            # Write the data rows
            for row in rows:
                csvwriter.writerow(row)

finally:
    # Close connection
    connection.close()
