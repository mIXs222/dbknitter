import pymysql
import csv

# Establish connection
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch')

# Create a cursor
cursor = connection.cursor()

# Execute SQL query
cursor.execute("""
    SELECT
        100.00 * SUM(CASE WHEN P_TYPE LIKE 'PROMO%' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT)
        ELSE 0
        END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE
    FROM
        lineitem,
        part
    WHERE
        L_PARTKEY = P_PARTKEY
        AND L_SHIPDATE >= '1995-09-01'
        AND L_SHIPDATE < '1995-10-01'
""")

# Fetch all rows
rows = cursor.fetchall()

# Write to CSV
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([i[0] for i in cursor.description])  # Write column headers
    writer.writerows(rows)  # Write data

# Close connection
connection.close()
