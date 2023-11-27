import pymysql
import csv

# Connection details
connection = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch')

# SQL query
sql_query = """
SELECT 
    c.C_CUSTKEY, 
    c.C_NAME, 
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE, 
    c.C_ACCTBAL, 
    n.N_NAME, 
    c.C_ADDRESS, 
    c.C_PHONE, 
    c.C_COMMENT
FROM 
    customer c
JOIN 
    orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN 
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN 
    nation n ON c.C_NATIONKEY = n.N_NATIONKEY
WHERE 
    l.L_RETURNFLAG = 'R' AND 
    o.O_ORDERDATE >= '1993-10-01' AND 
    o.O_ORDERDATE <= '1993-12-31'
GROUP BY 
    c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL, c.C_PHONE, n.N_NAME, c.C_ADDRESS, c.C_COMMENT
ORDER BY 
    REVENUE ASC, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL DESC;
"""

try:
    # Execute query
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        
        # Write results to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow([i[0] for i in cursor.description])
            # Write data
            writer.writerows(cursor.fetchall())
finally:
    connection.close()
