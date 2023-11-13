import mysql.connector
import csv

#establishing the connection
conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Executing the query
cursor.execute("""SELECT
                        C_CUSTKEY,
                        C_NAME,
                        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
                        C_ACCTBAL,
                        N_NAME,
                        C_ADDRESS,
                        C_PHONE,
                        C_COMMENT
                    FROM
                        customer,
                        orders,
                        lineitem,
                        nation
                    WHERE
                        C_CUSTKEY = O_CUSTKEY
                        AND L_ORDERKEY = O_ORDERKEY
                        AND O_ORDERDATE >= '1993-10-01'
                        AND O_ORDERDATE < '1994-01-01'
                        AND L_RETURNFLAG = 'R'
                        AND C_NATIONKEY = N_NATIONKEY
                    GROUP BY
                        C_CUSTKEY,
                        C_NAME,
                        C_ACCTBAL,
                        C_PHONE,
                        N_NAME,
                        C_ADDRESS,
                        C_COMMENT
                    ORDER BY
                        REVENUE, C_CUSTKEY, C_NAME, C_ACCTBAL DESC""")

#fetching the rows from the cursor object
result = cursor.fetchall()

#open a file for writing    
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)

    #write a row to the csv file
    for row in result:
        writer.writerow(row)

#close the cursor and connection   
cursor.close()
conn.close()
