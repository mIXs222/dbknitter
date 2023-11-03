import pandas as pd
from sqlalchemy import create_engine

database_username = 'root'
database_password = 'my-secret-pw'
database_ip       = 'mysql'
database_name     = 'tpch'
database_connection = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

query = ("""
SELECT
C_CUSTKEY,
C_NAME,
SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
C_ACCTBAL,
N_NAME,
C_ADDRESS,
C_PHONE,
C_COMMENT
FROM
customer,
orders,
lineitem,
nation
WHERE
C_CUSTKEY = O_CUSTKEY
AND L_ORDERKEY = O_ORDERKEY
AND O_ORDERDATE >= '1993-10-01'
AND O_ORDERDATE < '1994-01-01'
AND L_RETURNFLAG = 'R'
AND C_NATIONKEY = N_NATIONKEY
GROUP BY
C_CUSTKEY,
C_NAME,
C_ACCTBAL,
C_PHONE,
N_NAME,
C_ADDRESS,
C_COMMENT
ORDER BY
REVENUE, C_CUSTKEY, C_NAME, C_ACCTBAL DESC
""")

df = pd.read_sql_query(query, database_connection)
df.to_csv('query_output.csv', index=False)
