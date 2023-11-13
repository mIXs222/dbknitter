import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

def query_data():
    mydb = mysql.connector.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        database="tpch"
    )

    mycursor = mydb.cursor()

    mycursor.execute("""
    SELECT
        SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
    FROM
        lineitem,
        part
    WHERE
        P_PARTKEY = L_PARTKEY
        AND P_BRAND = 'Brand#23'
        AND P_CONTAINER = 'MED BAG'
        AND L_QUANTITY < (
            SELECT
                0.2 * AVG(L_QUANTITY)
            FROM
                lineitem
            WHERE
                L_PARTKEY = P_PARTKEY
        )
    """)

    result = mycursor.fetchall()
    df = pd.DataFrame(result, columns=['AVG_YEARLY'])

    # save DataFrame to csv
    df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    query_data()
