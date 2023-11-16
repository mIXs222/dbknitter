# query_script.py
import pymysql
import pandas as pd
from sqlalchemy import create_engine

# create a connection
conn = pymysql.connect(host='mysql',
                       user='root',
                       password='my-secret-pw',
                       db='tpch')

# create a cursor
cursor = conn.cursor()

# define the query
query = """
SELECT
    P_BRAND,
    P_TYPE,
    P_SIZE,
    COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM
    partsupp,
    part
WHERE
    P_PARTKEY = PS_PARTKEY
    AND P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND PS_SUPPKEY NOT IN (
        SELECT
            S_SUPPKEY
        FROM
            supplier
        WHERE
            S_COMMENT LIKE '%Customer%Complaints%'
    )
GROUP BY
    P_BRAND,
    P_TYPE,
    P_SIZE
ORDER BY
    SUPPLIER_CNT DESC,
    P_BRAND,
    P_TYPE,
    P_SIZE
"""

#execute query
df = pd.read_sql(query, conn)

# save the results to csv
df.to_csv('query_output.csv', index=False)

