# Python code to execute the query on mysql database

import pymysql
import csv

# define connection attributes for mysql database
mysql_conn = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Query for mysql database
mysql_query = """
SELECT
    L_ORDERKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
    O_ORDERDATE,
    O_SHIPPRIORITY
FROM
    customer,
    orders,
    lineitem
WHERE
    C_MKTSEGMENT = 'BUILDING'
    AND C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE < '1995-03-15'
    AND L_SHIPDATE > '1995-03-15'
GROUP BY
    L_ORDERKEY,
    O_ORDERDATE,
    O_SHIPPRIORITY
ORDER BY
    REVENUE DESC,
    O_ORDERDATE
"""

def fetch_data_from_mysql():
    # Connect to the mysql database
    connection = pymysql.connect(
        host=mysql_conn['host'],
        user=mysql_conn['user'],
        password=mysql_conn['password'],
        db=mysql_conn['db'],
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(mysql_query)
            return cursor.fetchall()
    finally:
        connection.close()

def save_to_csv(results, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])  # Write header
        writer.writerows(results)

def main():
    mysql_results = fetch_data_from_mysql()
    save_to_csv(mysql_results, 'query_output.csv')

if __name__ == "__main__":
    main()
