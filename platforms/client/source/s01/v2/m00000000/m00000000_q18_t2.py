import csv
import mysql.connector

DB_CONFIG = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch',
}

QUERY = """
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

def main():
    cnx = mysql.connector.connect(**DB_CONFIG)
    cursor = cnx.cursor()

    cursor.execute(QUERY)

    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([i[0] for i in cursor.description])   # write headers
        writer.writerows(cursor.fetchall())  # write data 

    cursor.close()
    cnx.close()

if __name__ == "__main__":
    main()
