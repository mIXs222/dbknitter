import pymysql
import csv
import datetime

# Database connection details
db_config = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch',
}

# Establishing the database connection
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

# SQL query as per the user's specification
query = """
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
    L_RETURNFLAG, 
    L_LINESTATUS
ORDER BY 
    L_RETURNFLAG, 
    L_LINESTATUS
"""

# Execute the query
cursor.execute(query)

# Fetching the results
results = cursor.fetchall()

# Writing results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    # Writing header
    csv_writer.writerow([i[0] for i in cursor.description])
    # Writing data
    for row in results:
        csv_writer.writerow(row)

# Closing the cursor and the connection
cursor.close()
conn.close()
