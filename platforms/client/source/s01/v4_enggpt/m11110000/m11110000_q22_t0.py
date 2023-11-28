import pymysql
import csv

# Connect to the MySQL database
conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4'
)

# Query for analyzing customer data
query = """
SELECT 
    SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE, 
    COUNT(*) AS NUMCUST,
    SUM(C_ACCTBAL) AS TOTACCTBAL
FROM 
    customer
WHERE 
    C_ACCTBAL > (
        SELECT AVG(C_ACCTBAL) 
        FROM customer 
        WHERE C_ACCTBAL > 0 AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
    )
    AND NOT EXISTS (
        SELECT * 
        FROM orders 
        WHERE customer.C_CUSTKEY = orders.O_CUSTKEY
    )
GROUP BY CNTRYCODE
ORDER BY CNTRYCODE
"""

try:
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Write results to file
        with open('query_output.csv', 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL'])  # Header
            for row in rows:
                csv_writer.writerow(row)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    conn.close()
