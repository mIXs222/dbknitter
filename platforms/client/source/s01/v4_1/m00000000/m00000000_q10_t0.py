import pymysql
import pandas as pd
from pymysql.cursors import DictCursor

#pymysql connection
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=DictCursor
)

try:
    with connection.cursor() as cursor:
        sql = """
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
                REVENUE DESC, C_CUSTKEY, C_NAME, C_ACCTBAL DESC
        """
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        df.to_csv("query_output.csv", index = False)
finally:
    connection.close()
