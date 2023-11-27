import pymysql
import pandas as pd

# Establish a connection to the MySQL server
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

try:
    # Create a cursor
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    # Execute the SQL query
    cursor.execute("""
    SELECT
        SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
    FROM
        lineitem,
        part
    WHERE
        P_PARTKEY = L_PARTKEY
        AND P_BRAND = 'Brand#23'
        AND P_CONTAINER = 'MED BAG'
        AND L_QUANTITY < (
            SELECT
                0.2 * AVG(L_QUANTITY)
            FROM
                lineitem
            WHERE
                L_PARTKEY = P_PARTKEY
        )
    """)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Convert to pandas dataframe
    df = pd.DataFrame(rows)

    # Write the output to a CSV file
    df.to_csv('query_output.csv', index=False)

finally:
    # Close the connection
    connection.close()
