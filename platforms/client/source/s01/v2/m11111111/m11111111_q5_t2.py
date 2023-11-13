from pymongo import MongoClient
import pandas as pd
from pandasql import sqldf

client = MongoClient("mongodb://mongodb:27017")
db = client['tpch']

def pysqldf(query):
    return sqldf(query, globals())

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

tables = ['nation', 'region', 'supplier', 'customer', 'orders', 'lineitem']

for table in tables:
    globals()[table] = pd.DataFrame(list(db[table].find()))

result = pysqldf(query)
result.to_csv('query_output.csv', index=False)
