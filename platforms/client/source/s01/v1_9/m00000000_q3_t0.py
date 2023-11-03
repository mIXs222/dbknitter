import csv
import mysql.connector

# Connection to the Database
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mycursor = mydb.cursor()

# SQL Query
mycursor.execute("""
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

result = mycursor.fetchall()

# Open a file for writing
outfile = open("query_output.csv", "w")
writer = csv.writer(outfile)

# Column Names
columns = [i[0] for i in mycursor.description]
writer.writerow(columns)

# Writing the data
for row in result:
  writer.writerow(row)

outfile.close()
