import pandas as pd
import pyodbc
import csv

conn_string = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=mysql;'  # ADJUST THIS
    r'DATABASE=tpch;'  # ADJUST THIS
    r'UID=root;'  # ADJUST THIS
    r'PWD=my-secret-pw'  # ADJUST THIS
)

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
conn = pyodbc.connect(conn_string)
query_output = pd.read_sql(sql_query, conn)

query_output.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
