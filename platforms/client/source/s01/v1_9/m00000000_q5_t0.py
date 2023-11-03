import csv
import mysql.connector

# Establish the connection to the MySQL server
cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql', database='tpch')

# Create a cursor object using the cursor() method
cursor = cnx.cursor()

# Execute the SQL command using the execute() method
cursor.execute("""
    SELECT
        N_NAME,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
    FROM
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    WHERE
        C_CUSTKEY = O_CUSTKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND L_SUPPKEY = S_SUPPKEY
        AND C_NATIONKEY = S_NATIONKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'ASIA'
        AND O_ORDERDATE >= '1990-01-01'
        AND O_ORDERDATE < '1995-01-01'
    GROUP BY
        N_NAME
    ORDER BY
        REVENUE DESC
""")

# Fetch all of the rows
rows = cursor.fetchall()

# Write to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(rows)

# Close the cursor and connection
cursor.close()
cnx.close()
