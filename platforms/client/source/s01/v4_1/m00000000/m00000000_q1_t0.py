import pymysql
import csv
import pandas as pd

# Connection Setup for MySQL
try:
    connection = pymysql.connect(
    host='mysql',
    user='root', 
    password='my-secret-pw',    
    db='tpch',
    charset='utf8mb4',
    # cursorclass=pymysql.cursors.DictCursor
    )

    # SQL Query Execution
    sql_query = """
    SELECT
        L_RETURNFLAG,
        L_LINESTATUS,
        SUM(L_QUANTITY) AS SUM_QTY,
        SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
        AVG(L_QUANTITY) AS AVG_QTY,
        AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
        AVG(L_DISCOUNT) AS AVG_DISC,
        COUNT(*) AS COUNT_ORDER
    FROM
        lineitem
    WHERE
        L_SHIPDATE <= '1998-09-02'
    GROUP BY
        L_RETURNFLAG,
        L_LINESTATUS
    ORDER BY
        L_RETURNFLAG,
    L_LINESTATUS
    """

    df = pd.read_sql_query(sql_query, connection)
    df.to_csv('query_output.csv',index = False)

except Exception as e:
    print("Error while connecting to database:", str(e))
finally:
    if connection:
        connection.close()
