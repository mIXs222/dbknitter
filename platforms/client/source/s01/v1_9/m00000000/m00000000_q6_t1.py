import csv
import mysql.connector

# Establish a connection to the database
db_connection = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

# Create a cursor object
cursor = db_connection.cursor()

# Execute the query
cursor.execute("""
    SELECT
        SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= '1994-01-01'
        AND L_SHIPDATE < '1995-01-01'
        AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
        AND L_QUANTITY < 24
""")

# Fetch data from the execution of the query
records = cursor.fetchall()

# Write the output to query_output.csv 
filename = "query_output.csv"
with open(filename, 'w') as csvfile:
    csvwriter = csv.writer(csvfile) 
    csvwriter.writerow(['REVENUE']) # write the header
    for row in records:
        csvwriter.writerow(row)

# Close the database connection
db_connection.close()
