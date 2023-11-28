# query_executer.py
import pymysql
import csv
from datetime import datetime

# Connection parameters
db_connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Connect to MySQL database with pymysql
connection = pymysql.connect(**db_connection_params)
try:
    # Cursor to execute query
    with connection.cursor() as cursor:
        # SQL query
        sql = """
        SELECT
            o.O_ORDERKEY,
            o.O_ORDERDATE,
            o.O_SHIPPRIORITY,
            SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
        FROM
            customer c,
            orders o,
            lineitem l
        WHERE
            c.C_MKTSEGMENT = 'BUILDING'
            AND c.C_CUSTKEY = o.O_CUSTKEY
            AND l.L_ORDERKEY = o.O_ORDERKEY
            AND o.O_ORDERDATE < '1995-03-15'
            AND l.L_SHIPDATE > '1995-03-15'
        GROUP BY
            o.O_ORDERKEY, o.O_ORDERDATE, o.O_SHIPPRIORITY
        ORDER BY
            revenue DESC, o.O_ORDERDATE ASC
        """
        cursor.execute(sql)

        # Write results to a csv file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write headers
            writer.writerow(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'revenue'])
            # Write data
            for row in cursor.fetchall():
                row = list(row)
                # Convert datetime objects to string in the format 'YYYY-MM-DD'
                row[1] = datetime.strftime(row[1], '%Y-%m-%d') if isinstance(row[1], datetime) else row[1]
                writer.writerow(row)
finally:
    connection.close()
