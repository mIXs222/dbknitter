import mysql.connector
import pandas as pd

def query_to_csv():
    mydb = mysql.connector.connect(
      host="mysql",
      user="root",
      password="my-secret-pw",
      database="tpch"
    )

    mycursor = mydb.cursor()

    mycursor.execute("""
    SELECT S_NAME, S_ADDRESS 
    FROM supplier, nation 
    WHERE S_SUPPKEY IN (
        SELECT PS_SUPPKEY
        FROM partsupp 
        WHERE PS_PARTKEY IN (
            SELECT P_PARTKEY
            FROM part
            WHERE P_NAME LIKE 'forest%'
        ) 
        AND PS_AVAILQTY > (
            SELECT 0.5 * SUM(L_QUANTITY)
            FROM lineitem
            WHERE L_PARTKEY = PS_PARTKEY 
            AND L_SUPPKEY = PS_SUPPKEY
            AND L_SHIPDATE >= '1994-01-01'
            AND L_SHIPDATE < '1995-01-01'
        )
    ) 
    AND S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'CANADA'
    ORDER BY S_NAME
    """)

    rows = mycursor.fetchall()

    df = pd.DataFrame(rows, columns = ['S_NAME', 'S_ADDRESS'])
    df.to_csv('query_output.csv', index = False)

query_to_csv()
