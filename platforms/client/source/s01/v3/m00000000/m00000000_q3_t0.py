import mysql.connector
import csv

cnx = mysql.connector.connect(user='root', password='my-secret-pw', 
                              host='mysql', database='tpch')

# Create a cursor object
cursor = cnx.cursor()

# Execute the query
cursor.execute("""
    SELECT
        L_ORDERKEY,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
        O_ORDERDATE,
        O_SHIPPRIORITY
    FROM
        customer,
        orders,
        lineitem
    WHERE
        C_MKTSEGMENT = 'BUILDING'
        AND C_CUSTKEY = O_CUSTKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_ORDERDATE < '1995-03-15'
        AND L_SHIPDATE > '1995-03-15'
    GROUP BY
        L_ORDERKEY,
        O_ORDERDATE,
        O_SHIPPRIORITY
    ORDER BY
        REVENUE DESC,
        O_ORDERDATE
""")

# Fetch results into a CSV file 'query_output.csv'
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([i[0] for i in cursor.description])  # write headers
    writer.writerows(cursor.fetchall())  # write data

# Close the cursor and disconnect from the server
cursor.close()
cnx.close()
