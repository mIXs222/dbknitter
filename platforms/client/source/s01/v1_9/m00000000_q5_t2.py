import mysql.connector
import pandas as pd

# create a connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')

# prepare the query
query = """
SELECT
    N_NAME,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND L_SUPPKEY = S_SUPPKEY
    AND C_NATIONKEY = S_NATIONKEY
    AND S_NATIONKEY = N_NATIONKEY
    AND N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'ASIA'
    AND O_ORDERDATE >= '1990-01-01'
    AND O_ORDERDATE < '1995-01-01'
GROUP BY
    N_NAME
ORDER BY
    REVENUE DESC
"""

# execute the query
df = pd.read_sql(query, cnx)

# write the result to csv
df.to_csv('query_output.csv', index=False)

# close the connection
cnx.close()
