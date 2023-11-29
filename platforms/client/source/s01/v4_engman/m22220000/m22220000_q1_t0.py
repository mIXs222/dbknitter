import pymysql
import csv

# Define the connection information for the MySQL database
mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connect to the MySQL database
mysql_connection = pymysql.connect(**mysql_config)
try:
    # Prepare a cursor object using cursor() method
    cursor = mysql_connection.cursor()

    # SQL query
    query = """
    SELECT
        L_RETURNFLAG,
        L_LINESTATUS,
        SUM(L_QUANTITY) AS SUM_QTY,
        SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
        AVG(L_QUANTITY) AS AVG_QTY,
        AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
        AVG(L_DISCOUNT) AS AVG_DISC,
        COUNT(*) AS COUNT_ORDER
    FROM
        lineitem
    WHERE
        L_SHIPDATE < '1998-09-02'
    GROUP BY
        L_RETURNFLAG,
        L_LINESTATUS
    ORDER BY
        L_RETURNFLAG,
        L_LINESTATUS;
    """

    # Execute the SQL command
    cursor.execute(query)

    # Fetch all the rows in a list of lists.
    results = cursor.fetchall()

    # Write results to a file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write the headers
        csvwriter.writerow([
            'L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE',
            'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC',
            'COUNT_ORDER'
        ])
        # Write the data rows
        for row in results:
            csvwriter.writerow(row)

except pymysql.Error as e:
    print("MySQL Error: %s" % str(e))

finally:
    # Close the connection
    if mysql_connection:
        mysql_connection.close()
