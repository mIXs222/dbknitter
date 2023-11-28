import pymysql
import csv
from datetime import datetime

# Define the connection parameters
mysql_conn_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql'
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)
cursor = mysql_conn.cursor()

# Query
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
    L_SHIPDATE <= '1998-09-02'
GROUP BY 
    L_RETURNFLAG, 
    L_LINESTATUS
ORDER BY 
    L_RETURNFLAG, 
    L_LINESTATUS;
"""

# Execute the query
cursor.execute(query)
results = cursor.fetchall()

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
    # Write headers
    csvwriter.writerow([i[0] for i in cursor.description])
    # Write data rows
    for row in results:
        csvwriter.writerow(row)

# Close the cursor and connection
cursor.close()
mysql_conn.close()
