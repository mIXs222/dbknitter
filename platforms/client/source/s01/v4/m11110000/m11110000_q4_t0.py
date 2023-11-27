# query.py
import pymysql
import csv

# Connection details
db_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Establish connection to MySQL instance
conn = pymysql.connect(
    host=db_info['host'],
    user=db_info['user'],
    password=db_info['password'],
    database=db_info['database']
)

# Prepare the SQL query
query = """
SELECT
    O_ORDERPRIORITY,
    COUNT(*) AS ORDER_COUNT
FROM
    orders
WHERE
    O_ORDERDATE >= '1993-07-01'
    AND O_ORDERDATE < '1993-10-01'
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            L_ORDERKEY = O_ORDERKEY
            AND L_COMMITDATE < L_RECEIPTDATE
    )
GROUP BY
    O_ORDERPRIORITY
ORDER BY
    O_ORDERPRIORITY
"""

# Execute the SQL query
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

# Write the output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])  # Writing header
    writer.writerows(results)

# Close connection
conn.close()
