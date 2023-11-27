import pymysql
import csv

# Connection details
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# SQL Query
sql_query = """
SELECT 
    S_NAME, 
    COUNT(*) AS NUMWAIT
FROM 
    supplier S
INNER JOIN 
    nation N ON S.S_NATIONKEY = N.N_NATIONKEY
INNER JOIN 
    lineitem L1 ON S.S_SUPPKEY = L1.L_SUPPKEY
INNER JOIN 
    orders O ON L1.L_ORDERKEY = O.O_ORDERKEY
WHERE 
    N_NAME = 'SAUDI ARABIA' AND
    O_ORDERSTATUS = 'F' AND
    L1.L_RECEIPTDATE > L1.L_COMMITDATE AND
    EXISTS (
        SELECT * 
        FROM lineitem L2
        WHERE 
            L2.L_ORDERKEY = L1.L_ORDERKEY AND 
            L2.L_SUPPKEY != L1.L_SUPPKEY
    ) AND NOT EXISTS (
        SELECT * 
        FROM lineitem L3
        WHERE 
            L3.L_ORDERKEY = L1.L_ORDERKEY AND 
            L3.L_SUPPKEY != L1.L_SUPPKEY AND
            L3.L_RECEIPTDATE > L1.L_COMMITDATE
    )
GROUP BY 
    S.S_NAME
ORDER BY 
    NUMWAIT DESC, 
    S_NAME ASC;
"""

try:
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        result = cursor.fetchall()
        
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['S_NAME', 'NUMWAIT'])
            for row in result:
                csvwriter.writerow(row)
finally:
    connection.close()
