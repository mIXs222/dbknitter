import mysql.connector
import csv

# establish a database connection
cnx = mysql.connector.connect(user='root', 
                              password='my-secret-pw', 
                              host='mysql', 
                              database='tpch')
                              
# prepare a cursor object
cursor = cnx.cursor()

# execute SQL query using execute() method.
cursor.execute("""
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
""")

# Fetch all rows using fetchall() method.
rows = cursor.fetchall()

# write rows into csv file
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(rows)

# disconnect from server
cnx.close()
