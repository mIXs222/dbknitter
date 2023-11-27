# query.py
import pymysql
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

try:
    # Cursor to execute query
    cursor = mysql_conn.cursor()

    # SQL query
    query = """
    SELECT L_SHIPMODE, O_ORDERPRIORITY, COUNT(*) as TOTAL_LATE_LINEITEMS
    FROM lineitem
    INNER JOIN orders ON L_ORDERKEY = O_ORDERKEY
    WHERE L_SHIPMODE IN ('MAIL', 'SHIP')
    AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
    AND L_COMMITDATE < L_RECEIPTDATE
    AND L_SHIPDATE < L_COMMITDATE
    AND O_ORDERPRIORITY IN ('URGENT', 'HIGH')
    GROUP BY L_SHIPMODE, O_ORDERPRIORITY
    
    UNION ALL
    
    SELECT L_SHIPMODE, 'OTHER' as O_ORDERPRIORITY, COUNT(*) as TOTAL_LATE_LINEITEMS
    FROM lineitem
    INNER JOIN orders ON L_ORDERKEY = O_ORDERKEY
    WHERE L_SHIPMODE IN ('MAIL', 'SHIP')
    AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
    AND L_COMMITDATE < L_RECEIPTDATE
    AND L_SHIPDATE < L_COMMITDATE
    AND O_ORDERPRIORITY NOT IN ('URGENT', 'HIGH')
    GROUP BY L_SHIPMODE, O_ORDERPRIORITY
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Write to csv file
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['SHIPMODE', 'ORDERPRIORITY', 'TOTAL_LATE_LINEITEMS'])
        # Write data
        for row in rows:
            writer.writerow(row)

finally:
    # Close the connection
    mysql_conn.close()
