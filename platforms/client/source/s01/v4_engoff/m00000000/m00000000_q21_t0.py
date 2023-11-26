import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    # Create a cursor object to perform the query
    cursor = connection.cursor()
    
    # The query to find suppliers
    query = """
    SELECT s.S_SUPPKEY, s.S_NAME FROM supplier s
    JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
    JOIN orders o ON o.O_ORDERSTATUS = 'F'
    JOIN lineitem l ON l.L_SUPPKEY = s.S_SUPPKEY AND l.L_ORDERKEY = o.O_ORDERKEY
    WHERE n.N_NAME = 'SAUDI ARABIA'
    AND l.L_COMMITDATE < l.L_RECEIPTDATE
    AND NOT EXISTS (
        SELECT * FROM lineitem l2
        WHERE l2.L_ORDERKEY = l.L_ORDERKEY
        AND l2.L_SUPPKEY <> l.L_SUPPKEY
        AND l2.L_COMMITDATE >= l2.L_RECEIPTDATE
    )
    GROUP BY s.S_SUPPKEY, s.S_NAME;
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the records
    result = cursor.fetchall()

    # Save the query's output to 'query_output.csv'
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Assuming you want headers as well
        writer.writerow(['S_SUPPKEY', 'S_NAME'])
        for row in result:
            writer.writerow(row)

finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()
