import mysql.connector
import pandas as pd

# MySQL connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
cursor = cnx.cursor()

# SQL query
query = """
SELECT
    L_ORDERKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
    O_ORDERDATE,
    O_SHIPPRIORITY
FROM
    customer,
    orders,
    lineitem
WHERE
    C_MKTSEGMENT = 'BUILDING'
    AND C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE < '1995-03-15'
    AND L_SHIPDATE > '1995-03-15'
GROUP BY
    L_ORDERKEY,
    O_ORDERDATE,
    O_SHIPPRIORITY
ORDER BY
    REVENUE DESC,
    O_ORDERDATE
"""

# Execute the query
cursor.execute(query)

# Fetch all rows from the cursor
rows = cursor.fetchall()

# Specify columns of the DataFrame
columns = ['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']

# Create DataFrame from rows
df = pd.DataFrame(rows, columns=columns)

# Write DataFrame to CSV file
df.to_csv('query_output.csv', index=False)

# Close cursor & connection
cursor.close()
cnx.close()
