import pymysql
import csv

# Establish connection to the MySQL database
connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Create a cursor object
cursor = connection.cursor()

# SQL query to be executed
sql_query = """
SELECT
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
FROM
    lineitem
JOIN
    part ON lineitem.L_PARTKEY = part.P_PARTKEY
WHERE
    (
        P_BRAND = 'Brand#12'
        AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND L_QUANTITY BETWEEN 1 AND 11
        AND P_SIZE BETWEEN 1 AND 5
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    ) OR (
        P_BRAND = 'Brand#23'
        AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND L_QUANTITY BETWEEN 10 AND 20
        AND P_SIZE BETWEEN 1 AND 10
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    ) OR (
        P_BRAND = 'Brand#34'
        AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND L_QUANTITY BETWEEN 20 AND 30
        AND P_SIZE BETWEEN 1 AND 15
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )
"""

# Execute the SQL query
cursor.execute(sql_query)

# Fetch all the records
records = cursor.fetchall()

# Write records to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['revenue'])
    for row in records:
        writer.writerow(row)

# Close the cursor and connection
cursor.close()
connection.close()
