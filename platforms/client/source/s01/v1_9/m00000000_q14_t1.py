import mysql.connector
import csv
from datetime import datetime

# Establish a connection to the database
cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')

# Prepare a cursor
cursor = cnx.cursor()

# Prepare the SQL query
query = ("SELECT 100.00 * SUM(CASE WHEN P_TYPE LIKE 'PROMO%' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT) ELSE 0 END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE FROM lineitem, part WHERE L_PARTKEY = P_PARTKEY AND L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE < '1995-10-01'")

# Execute the query
cursor.execute(query)

# Fetch all the rows
rows = cursor.fetchall()

# Prepare the CSV file
with open('query_output.csv','w',newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["PROMO_REVENUE"]) # Write the header
    for row in rows:
        writer.writerow(row) # Write each result row

# Close the cursor and connection
cursor.close()
cnx.close()
