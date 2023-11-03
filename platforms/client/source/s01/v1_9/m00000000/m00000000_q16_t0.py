import pymysql
import csv

# Establish database connection
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Prepare a cursor object using cursor() method
cursor = connection.cursor()

# Define the SQL query
sql_query = """
    SELECT
        P_BRAND,
        P_TYPE,
        P_SIZE,
        COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
    FROM
        partsupp,
        part
    WHERE
        P_PARTKEY = PS_PARTKEY
        AND P_BRAND <> 'Brand#45'
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

# Execute the SQL command
cursor.execute(sql_query)

# Fetch all the rows in a list of lists
results = cursor.fetchall()

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])  # writing column headers
    for row in results:
        writer.writerow(row)

# Disconnect from server
connection.close()
