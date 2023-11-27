import pymysql
import csv

# Connection parameters
db_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4',
}

# Connect to the mysql database
conn = pymysql.connect(**db_params)
cursor = conn.cursor()

# Execute the query
query = """
SELECT
    PS.PS_PARTKEY,
    SUM(PS.PS_SUPPLYCOST * PS.PS_AVAILQTY) AS value
FROM
    partsupp PS
JOIN
    supplier S ON PS.PS_SUPPKEY = S.S_SUPPKEY
JOIN
    nation N ON S.S_NATIONKEY = N.N_NATIONKEY
WHERE
    N.N_NAME = 'GERMANY'
GROUP BY
    PS.PS_PARTKEY
HAVING
    SUM(PS.PS_SUPPLYCOST * PS.PS_AVAILQTY) > (
        SELECT
            SUM(PS.PS_SUPPLYCOST * PS.PS_AVAILQTY) * 0.0001
        FROM
            partsupp PS
        JOIN
            supplier S ON PS.PS_SUPPKEY = S.S_SUPPKEY
        JOIN
            nation N ON S.S_NATIONKEY = N.N_NATIONKEY
        WHERE
            N.N_NAME = 'GERMANY'
    )
ORDER BY
    value DESC;
"""

cursor.execute(query)
result_set = cursor.fetchall()

# Write the query result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["PS_PARTKEY", "VALUE"])
    for row in result_set:
        writer.writerow(row)

# Close the database connection
cursor.close()
conn.close()
