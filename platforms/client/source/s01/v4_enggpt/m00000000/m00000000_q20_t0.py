# File: query_execute.py

import pymysql
import csv

def query_mysql():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
    cursor = connection.cursor()

    query = """
    SELECT S_NAME, S_ADDRESS 
    FROM supplier, nation
    WHERE S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'CANADA'
    AND S_SUPPKEY IN (
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
            AND L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
        )
    )
    ORDER BY S_NAME ASC;
    """

    try:
        cursor.execute(query)
        results = cursor.fetchall()
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['S_NAME', 'S_ADDRESS'])
            for row in results:
                csvwriter.writerow(row)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        connection.close()

if __name__ == '__main__':
    query_mysql()
