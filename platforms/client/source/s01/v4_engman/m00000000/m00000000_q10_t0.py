import pymysql
import csv

# Database connection parameters.
db_params = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql'
}

query = """
SELECT
    c.C_CUSTKEY,
    c.C_NAME,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue_lost,
    c.C_ACCTBAL,
    n.N_NAME as nation,
    c.C_ADDRESS,
    c.C_PHONE,
    c.C_COMMENT
FROM
    customer AS c
JOIN
    orders AS o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN
    lineitem AS l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN
    nation AS n ON c.C_NATIONKEY = n.N_NATIONKEY
WHERE
    l.L_RETURNFLAG = 'R'
    AND o.O_ORDERDATE >= '1993-10-01'
    AND o.O_ORDERDATE < '1994-01-01'
GROUP BY
    c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL, nation, c.C_ADDRESS, c.C_PHONE, c.C_COMMENT
ORDER BY
    revenue_lost ASC,
    c.C_CUSTKEY ASC,
    c.C_NAME ASC,
    c.C_ACCTBAL DESC
"""

# Establish the database connection.
conn = pymysql.connect(
    host=db_params['host'],
    user=db_params['user'],
    password=db_params['password'],
    database=db_params['database']
)

# Execute the query.
with conn.cursor() as cursor:
    cursor.execute(query)
    records = cursor.fetchall()

    # Write the results to a CSV file.
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        # Write header row.
        writer.writerow(['C_CUSTKEY', 'C_NAME', 'REVENUE_LOST', 'C_ACCTBAL', 'NATION', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'])
        # Write data rows.
        for row in records:
            writer.writerow(row)

# Close the database connection.
conn.close()
