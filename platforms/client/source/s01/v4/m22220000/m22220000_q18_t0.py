import pymysql
import csv

# Connection information
db_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to the MySQL database
conn = pymysql.connect(
    host=db_info['host'],
    user=db_info['user'],
    password=db_info['password'],
    database=db_info['database']
)
cursor = conn.cursor()

# Execute the query
query = """
SELECT
    C_NAME,
    C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE,
    SUM(L_QUANTITY)
FROM
    customer,
    orders,
    lineitem
WHERE
    O_ORDERKEY IN (
        SELECT
            L_ORDERKEY
        FROM
            lineitem
        GROUP BY
            L_ORDERKEY HAVING
            SUM(L_QUANTITY) > 300
    )
AND C_CUSTKEY = O_CUSTKEY
AND O_ORDERKEY = L_ORDERKEY
GROUP BY
    C_NAME,
    C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE
ORDER BY
    O_TOTALPRICE DESC,
    O_ORDERDATE
"""

cursor.execute(query)
results = cursor.fetchall()

# Write results to file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    # Write header
    writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM_QUANTITY'])
    # Write data rows
    for row in results:
        writer.writerow(row)

# Close connection
cursor.close()
conn.close()
