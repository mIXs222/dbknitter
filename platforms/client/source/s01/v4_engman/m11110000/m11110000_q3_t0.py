# query.py
import pymysql
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

# SQL Query
query = """
SELECT
    o.O_ORDERKEY,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
    o.O_ORDERDATE,
    o.O_SHIPPRIORITY
FROM
    orders AS o
JOIN
    lineitem AS l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN
    customer AS c ON o.O_CUSTKEY = c.C_CUSTKEY
WHERE
    o.O_ORDERDATE < '1995-03-05'
    AND l.L_SHIPDATE > '1995-03-15'
    AND c.C_MKTSEGMENT = 'BUILDING'
GROUP BY
    o.O_ORDERKEY, o.O_ORDERDATE, o.O_SHIPPRIORITY
ORDER BY
    REVENUE DESC;
"""

try:
    with mysql_conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    # Write results to csv file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
        for row in result:
            writer.writerow(row)

finally:
    mysql_conn.close()
