import pymysql
import csv

# Establish a connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4')

try:
    # Get a cursor
    cursor = connection.cursor()

    # SQL query to execute
    sql = """
        SELECT 
            o.O_ORDERKEY,
            o.O_ORDERDATE,
            o.O_SHIPPRIORITY,
            SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
        FROM 
            customer c
        JOIN 
            orders o ON c.C_CUSTKEY = o.O_CUSTKEY
        JOIN 
            lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
        WHERE 
            c.C_MKTSEGMENT = 'BUILDING' AND 
            o.O_ORDERDATE < '1995-03-15' AND 
            l.L_SHIPDATE > '1995-03-15'
        GROUP BY 
            o.O_ORDERKEY, o.O_ORDERDATE, o.O_SHIPPRIORITY
        ORDER BY 
            revenue DESC, o.O_ORDERDATE ASC
    """

    cursor.execute(sql)
    rows = cursor.fetchall()

    # Write to CSV
    with open('query_output.csv', 'w', newline='') as file:
        csv_writer = csv.writer(file)
        # Write header
        csv_writer.writerow(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'REVENUE'])
        # Write data
        for row in rows:
            csv_writer.writerow(row)
finally:
    # Close the connection
    connection.close()
