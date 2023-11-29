import pymysql
import csv

# Establishing connection to MySQL
conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

try:
    with conn.cursor() as cursor:
        # Forming the SQL query
        sql = """
            SELECT o.O_ORDERKEY,
                   SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as REVENUE,
                   o.O_ORDERDATE,
                   o.O_SHIPPRIORITY
            FROM orders o
            JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
            JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
            WHERE o.O_ORDERDATE < '1995-03-05'
              AND l.L_SHIPDATE > '1995-03-15'
              AND c.C_MKTSEGMENT = 'BUILDING'
            GROUP BY o.O_ORDERKEY, o.O_ORDERDATE, o.O_SHIPPRIORITY
            ORDER BY REVENUE DESC;
        """
        
        # Executing query
        cursor.execute(sql)
        
        # Writing results to CSV
        with open('query_output.csv', mode='w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
            for row in cursor.fetchall():
                csv_writer.writerow(row)
finally:
    conn.close()
