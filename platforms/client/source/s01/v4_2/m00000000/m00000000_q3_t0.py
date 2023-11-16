# Required Libraries
import pymysql
import pandas as pd

# Connection Settings
host = 'mysql'
user = 'root'
password = 'my-secret-pw'
database = "tpch"

# Query 
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

# Connect to the database
connection = pymysql.connect(host=host,
                             user=user,
                             password=password,
                             db=database)

# Read the table
df = pd.read_sql(query, connection)

# Save to a csv file
df.to_csv('query_output.csv', index=False)

# Close a connection
connection.close()
