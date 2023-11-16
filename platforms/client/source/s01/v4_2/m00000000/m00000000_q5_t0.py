import pymysql
import pandas as pd

db_conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
        )

cursor = db_conn.cursor()
query = '''
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
    '''
cursor.execute(query)

df = pd.DataFrame(cursor.fetchall())
df.to_csv('query_output.csv', index=False)

cursor.close()
db_conn.close()
