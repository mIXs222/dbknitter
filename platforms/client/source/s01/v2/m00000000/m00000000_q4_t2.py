import pandas as pd
import mysql.connector

def connect_mysql():
    mydb = mysql.connector.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        database="tpch"
    )
    return mydb

def execute_query(mydb):
    mycursor = mydb.cursor()

    query = """SELECT O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT
    FROM orders
    WHERE O_ORDERDATE >= '1993-07-01'
        AND O_ORDERDATE < '1993-10-01'
        AND EXISTS (SELECT *
            FROM lineitem
            WHERE L_ORDERKEY = O_ORDERKEY
                AND L_COMMITDATE < L_RECEIPTDATE
            )
    GROUP BY O_ORDERPRIORITY
    ORDER BY O_ORDERPRIORITY"""

    mycursor.execute(query)
    result = mycursor.fetchall()
    return result

def write_to_csv(result):
    df = pd.DataFrame(result, columns=["O_ORDERPRIORITY", "ORDER_COUNT"])
    df.to_csv('query_output.csv', index=False)

def main():
    mydb = connect_mysql()
    result = execute_query(mydb)
    write_to_csv(result)

if __name__ == "__main__":
    main()
