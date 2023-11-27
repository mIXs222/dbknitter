import mysql.connector
import csv

def execute_query_and_save_data():

    # Connect to the MySQL server
    mydb = mysql.connector.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        database="tpch"
    )

    # prepare a cursor object 
    cursor = mydb.cursor()

    query = """SELECT SUPP_NATION, CUST_NATION, L_YEAR, SUM(VOLUME) AS REVENUE FROM 
        (SELECT N1.N_NAME AS SUPP_NATION, N2.N_NAME AS CUST_NATION, 
        strftime('%Y', L_SHIPDATE) AS L_YEAR, 
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME 
        FROM supplier, lineitem, orders, customer, nation n1, nation n2 
        WHERE S_SUPPKEY = L_SUPPKEY AND O_ORDERKEY = L_ORDERKEY
        AND C_CUSTKEY = O_CUSTKEY AND S_NATIONKEY = N1.N_NATIONKEY 
        AND C_NATIONKEY = N2.N_NATIONKEY AND ((N1.N_NAME = 'JAPAN' AND N2.N_NAME = 'INDIA')
        OR (N1.N_NAME = 'INDIA' AND N2.N_NAME = 'JAPAN')) 
        AND L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31') AS SHIPPING 
        GROUP BY SUPP_NATION, CUST_NATION, L_YEAR 
        ORDER BY SUPP_NATION, CUST_NATION, L_YEAR """

    cursor.execute(query)

    # Fetch all the records
    result = cursor.fetchall()

    # Write data to file
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(result)

execute_query_and_save_data()
