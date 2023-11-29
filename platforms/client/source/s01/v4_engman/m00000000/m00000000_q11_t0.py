import pymysql
import csv

# Connection details
connection_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# Connect to the MySQL database
connection = pymysql.connect(**connection_details)

# Prepare the SQL query
query = """
    SELECT ps.PS_PARTKEY, SUM(ps.PS_AVAILQTY * ps.PS_SUPPLYCOST) AS value
    FROM partsupp ps
    INNER JOIN supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY
    INNER JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
    WHERE n.N_NAME = 'GERMANY'
    GROUP BY ps.PS_PARTKEY
    HAVING SUM(ps.PS_AVAILQTY * ps.PS_SUPPLYCOST) > 0.0001 * (
        SELECT SUM(ps_inner.PS_AVAILQTY * ps_inner.PS_SUPPLYCOST)
        FROM partsupp ps_inner
    )
    ORDER BY value DESC;
"""

# Execute the query and fetch the results
try:
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()

        # Write data to csv
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['PS_PARTKEY', 'VALUE'])
            for row in data:
                writer.writerow(row)
finally:
    connection.close()
