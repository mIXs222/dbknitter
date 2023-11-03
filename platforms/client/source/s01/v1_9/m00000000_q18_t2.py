import mysql.connector
import csv

# function to connect to database and execute query
def execute_query(query):
    try:
        # establish a database connection
        cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
        cursor = cnx.cursor()
        cursor.execute(query)
        # fetch all rows from the last executed statement
        result = cursor.fetchall()
        cursor.close()
        cnx.close()
        return result
    except mysql.connector.Error as error:
        print("Failed to execute query in MySQL: {}".format(error))

# format SQL query
sql_query = """
    SELECT
        C_NAME,
        C_CUSTKEY,
        O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE,
        SUM(L_QUANTITY)
    FROM
        customer,
        orders,
        lineitem
    WHERE
        O_ORDERKEY IN (
        SELECT
            L_ORDERKEY
        FROM
            lineitem
        GROUP BY
            L_ORDERKEY HAVING
            SUM(L_QUANTITY) > 300
        )
    AND C_CUSTKEY = O_CUSTKEY
    AND O_ORDERKEY = L_ORDERKEY
    GROUP BY
        C_NAME,
        C_CUSTKEY,
        O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE
    ORDER BY
        O_TOTALPRICE DESC,
        O_ORDERDATE
"""

# call the function to execute query and get the data
data = execute_query(sql_query)

# write to csv
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)
