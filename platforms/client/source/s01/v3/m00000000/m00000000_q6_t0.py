import csv
import mysql.connector

# Create connection to MySQL
cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql', db='tpch')
cursor = cnx.cursor()

# Execute the query
query = ("SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE "
         "FROM lineitem "
         "WHERE L_SHIPDATE >= '1994-01-01' "
         "AND L_SHIPDATE < '1995-01-01' "
         "AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01 "
         "AND L_QUANTITY < 24")
cursor.execute(query)

# Write the result to the .csv file
with open('query_output.csv', 'w') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['REVENUE'])  # Header
    for result in cursor:
        csv_writer.writerow(result)

# Close the cursor and connection
cursor.close()
cnx.close()
