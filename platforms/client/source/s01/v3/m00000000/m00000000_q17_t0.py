import csv
import mysql.connector

def run_query():
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

    results = mycursor.fetchall()

    write_to_csv(results)

def write_to_csv(results):
    with open('query_output.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(results)

if __name__ == "__main__":
    run_query()
