import csv
import mysql.connector

# Connect to the MySQL database
mysql_db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mysql_cursor = mysql_db.cursor()

# Execute the SQL query
mysql_cursor.execute("""
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
""")

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["P_BRAND", "P_TYPE", "P_SIZE", "SUPPLIER_CNT"])  # write header
    for row in mysql_cursor:
        writer.writerow(row)

# Close the cursor and disconnect from the server
mysql_cursor.close()
mysql_db.close()
