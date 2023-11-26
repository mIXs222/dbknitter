# query.py
import csv
import pymysql

# Connection info
db_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# SQL query text
sql = """
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
    L_SHIPDATE < '1998-09-02'
GROUP BY
    L_RETURNFLAG, L_LINESTATUS
ORDER BY
    L_RETURNFLAG, L_LINESTATUS;
"""

# Connecting to the MySQL database server
conn = pymysql.connect(**db_info)
cur = conn.cursor()

# Executing the SQL query
cur.execute(sql)

# Fetch the results
result_set = cur.fetchall()

# Writing results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Writing header
    csvwriter.writerow([i[0] for i in cur.description])
    # Writing the data rows
    csvwriter.writerows(result_set)

# Closing the cursor and the connection
cur.close()
conn.close()
