# query.py
import pymysql
import csv

# Define the query
query = """
SELECT
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM
    lineitem
INNER JOIN
    part ON P_PARTKEY = L_PARTKEY
WHERE
    (
        P_BRAND = 'Brand#12'
        AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND L_QUANTITY BETWEEN 1 AND 11
        AND P_SIZE BETWEEN 1 AND 5
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )
    OR
    (
        P_BRAND = 'Brand#23'
        AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND L_QUANTITY BETWEEN 10 AND 20
        AND P_SIZE BETWEEN 1 AND 10
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )
    OR
    (
        P_BRAND = 'Brand#34'
        AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND L_QUANTITY BETWEEN 20 AND 30
        AND P_SIZE BETWEEN 1 AND 15
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )
"""

# Connect to the MySQL server
conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

try:
    with conn.cursor() as cursor:
        cursor.execute(query)

        # Write the output to 'query_output.csv'
        with open('query_output.csv', 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write header
            csv_writer.writerow([i[0] for i in cursor.description])
            # Write the data
            csv_writer.writerows(cursor)
finally:
    conn.close()
