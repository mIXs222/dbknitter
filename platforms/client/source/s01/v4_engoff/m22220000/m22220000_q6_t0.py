import pymysql
import csv

# Establish the connection to the MySQL database
try:
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    # Create a cursor object
    cursor = connection.cursor()

    # Query to fetch the desired data
    query = """
    SELECT
        SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS potential_revenue_increase
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= '1994-01-01'
        AND L_SHIPDATE < '1995-01-01'
        AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
        AND L_QUANTITY < 24;
    """

    # Execute the SQL query
    cursor.execute(query)

    # Fetch all the rows
    result = cursor.fetchall()

    # Write the result to a CSV file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write the header
        writer.writerow([i[0] for i in cursor.description])
        # Write the data
        for row in result:
            writer.writerow(row)

except pymysql.Error as e:
    print(f"Error connecting to MySQL Database: {e}")
finally:
    if connection:
        # Close the connection
        cursor.close()
        connection.close()
