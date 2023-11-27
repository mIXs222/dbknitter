import csv
import mysql.connector

def execute_query():
    mydb = mysql.connector.connect(
        host="mysql",
        user="root",
        password="my_secret_pw",
        database="tpch"
    )
    
    mycursor = mydb.cursor()
  
    mycursor.execute(
        """
        SELECT
            O_ORDERPRIORITY,
            COUNT(*) AS ORDER_COUNT
        FROM
            orders
        WHERE
            O_ORDERDATE >= '1993-07-01'
            AND O_ORDERDATE < '1993-10-01'
            AND EXISTS (
                SELECT
                    *
                FROM
                    lineitem
                WHERE
                    L_ORDERKEY = O_ORDERKEY
                    AND L_COMMITDATE < L_RECEIPTDATE
                )
        GROUP BY
            O_ORDERPRIORITY
        ORDER BY
            O_ORDERPRIORITY
        """
    )
    
    rows = mycursor.fetchall()
    
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(rows)

if __name__ == "__main__":
    execute_query()
