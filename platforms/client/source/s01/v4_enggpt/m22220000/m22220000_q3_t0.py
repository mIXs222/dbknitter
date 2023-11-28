import pymysql
import csv

# Define connection info
db_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to MySQL
connection = pymysql.connect(host=db_info['host'],
                             user=db_info['user'],
                             password=db_info['password'],
                             database=db_info['database'])

try:
    # Create a cursor object
    cursor = connection.cursor()

    # SQL query
    query = """
    SELECT
        O_ORDERKEY,
        O_ORDERDATE,
        O_SHIPPRIORITY,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
    FROM
        customer
    INNER JOIN
        orders ON C_CUSTKEY = O_CUSTKEY
    INNER JOIN
        lineitem ON L_ORDERKEY = O_ORDERKEY
    WHERE
        C_MKTSEGMENT = 'BUILDING'
        AND O_ORDERDATE < '1995-03-15'
        AND L_SHIPDATE > '1995-03-15'
    GROUP BY
        O_ORDERKEY,
        O_ORDERDATE,
        O_SHIPPRIORITY
    ORDER BY
        revenue DESC,
        O_ORDERDATE
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the records
    results = cursor.fetchall()

    # Write results to CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'REVENUE'])  # Column headers
        csvwriter.writerows(results)

finally:
    # Close the connection
    connection.close()
