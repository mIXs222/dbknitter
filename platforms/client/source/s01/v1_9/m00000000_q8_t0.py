import pandas as pd
from sqlalchemy import create_engine

# create a connection to the database
engine = create_engine('mysql+mysqlconnector://root:my-secret-pw@mysql/tpch')

# the SQL query
query = """
SELECT
    O_YEAR,
    SUM(CASE WHEN NATION = 'INDIA' THEN VOLUME ELSE 0 END) / SUM(VOLUME) AS MKT_SHARE
FROM
    (
    SELECT
        YEAR(O_ORDERDATE) AS O_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,
        N2.N_NAME AS NATION
    FROM
        PART,
        SUPPLIER,
        LINEITEM,
        ORDERS,
        CUSTOMER,
        NATION AS N1,
        NATION AS N2,
        REGION
    WHERE
        P_PARTKEY = L_PARTKEY
        AND S_SUPPKEY = L_SUPPKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_CUSTKEY = C_CUSTKEY
        AND C_NATIONKEY = N1.N_NATIONKEY
        AND N1.N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'ASIA'
        AND S_NATIONKEY = N2.N_NATIONKEY
        AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
        AND P_TYPE = 'SMALL PLATED COPPER'
    ) AS ALL_NATIONS
GROUP BY
    O_YEAR
ORDER BY
    O_YEAR
"""

# execute the query and fetch the data
data = pd.read_sql_query(query, engine)

# write the data to a CSV file
data.to_csv('query_output.csv', index=False)
