# Python Code (code.py)

import pymysql
import pandas as pd
from sqlalchemy import create_engine

# Establish a connection to the mysql database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
cursor = mysql_conn.cursor()

# Execute Query
query = '''
    SELECT
        C_COUNT,
        COUNT(*) AS CUSTDIST
    FROM
        (
        SELECT
            C_CUSTKEY,
            COUNT(O_ORDERKEY) AS C_COUNT
        FROM
            customer LEFT OUTER JOIN orders ON
            C_CUSTKEY = O_CUSTKEY
            AND O_COMMENT NOT LIKE '%pending%deposits%'
        GROUP BY
            C_CUSTKEY
        )   C_ORDERS
    GROUP BY
        C_COUNT
    ORDER BY
        CUSTDIST DESC,
        C_COUNT DESC
'''

df = pd.read_sql(query, mysql_conn)

# Write data to csv file
df.to_csv('query_output.csv', index=False)
