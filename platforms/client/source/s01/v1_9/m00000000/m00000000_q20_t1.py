import mysql.connector
import csv

# Connect to the database
cnx = mysql.connector.connect(user='root', password='my-secret-pw',  
                              host='mysql', database='tpch')

cursor = cnx.cursor()

query = ("SELECT S_NAME, S_ADDRESS FROM supplier, nation WHERE S_SUPPKEY IN (SELECT PS_SUPPKEY FROM partsupp WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%') AND PS_AVAILQTY > (SELECT 0.5 * SUM(L_QUANTITY) FROM lineitem WHERE L_PARTKEY = PS_PARTKEY AND L_SUPPKEY = PS_SUPPKEY AND L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01')) AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'CANADA' ORDER BY S_NAME")

cursor.execute(query)

# Export query results to csv
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([i[0] for i in cursor.description])  # write headers
    writer.writerows(cursor.fetchall())  # write data

cursor.close()
cnx.close()
