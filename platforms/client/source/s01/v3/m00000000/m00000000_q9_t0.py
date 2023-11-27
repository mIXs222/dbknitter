import mysql.connector
import csv

def get_data():
    connection = mysql.connector.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch'
    )

    cursor = connection.cursor()

    query = """
        SELECT
            NATION,
            O_YEAR,
            SUM(AMOUNT) AS SUM_PROFIT
        FROM
            (
            SELECT
                N_NAME AS NATION,
                EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR,
                L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT
            FROM
                part,
                supplier,
                lineitem,
                partsupp,
                orders,
                nation
            WHERE
                S_SUPPKEY = L_SUPPKEY
                AND PS_SUPPKEY = L_SUPPKEY
                AND PS_PARTKEY = L_PARTKEY
                AND P_PARTKEY = L_PARTKEY
                AND O_ORDERKEY = L_ORDERKEY
                AND S_NATIONKEY = N_NATIONKEY
                AND P_NAME LIKE '%dim%'
            ) AS PROFIT
        GROUP BY
            NATION,
            O_YEAR
        ORDER BY
            NATION,
            O_YEAR DESC
    """
    
    cursor.execute(query)
    result = cursor.fetchall()
    
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(result)

    cursor.close()
    connection.close()

get_data()
