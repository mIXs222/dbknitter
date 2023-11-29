import pymysql
import csv

# Define query to execute
query = """
    SELECT s.S_NAME, 
           sum(ps.PS_AVAILQTY) as total_parts
    FROM supplier s
    INNER JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
    INNER JOIN part p ON ps.PS_PARTKEY = p.P_PARTKEY
    INNER JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
    INNER JOIN lineitem l ON ps.PS_PARTKEY = l.L_PARTKEY AND ps.PS_SUPPKEY = l.L_SUPPKEY
    WHERE n.N_NAME = 'CANADA'
    AND p.P_NAME LIKE 'forest%'
    AND l.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
    GROUP BY s.S_SUPPKEY
    HAVING total_parts > 0.5 * (
        SELECT sum(l2.L_QUANTITY)
        FROM part p2
        INNER JOIN lineitem l2 ON p2.P_PARTKEY = l2.L_PARTKEY
        WHERE p2.P_NAME LIKE 'forest%'
        AND l2.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
    )
"""

# Establish a database connection
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Create a cursor object
cursor = conn.cursor()

# Execute the query
cursor.execute(query)

# Fetch all the records
result = cursor.fetchall()

# Write output to csv file
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    # Write the header
    csv_writer.writerow(['S_NAME', 'TOTAL_PARTS'])
    # Write the data rows
    for row in result:
        csv_writer.writerow(row)

# Close the cursor and the connection
cursor.close()
conn.close()
