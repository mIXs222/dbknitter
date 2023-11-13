import pandas as pd
import mysql.connector
import csv

# Define your databases connections here, in our case only mysql exist, for others follow the same formation.
# Important note, the password, host and user for your mysql connection should be confidential and usually are inserted as environment variables.
cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')

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
    O_ORDERDATE """

df = pd.read_sql(query, cnx)
df.to_csv('query_output.csv')
