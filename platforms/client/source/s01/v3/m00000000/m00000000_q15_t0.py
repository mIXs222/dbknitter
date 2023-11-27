import mysql.connector
import pandas as pd

# Create a connection to MySQL
cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')

# Create a cursor object
cursor = cnx.cursor()

# Execute the query
query = """
        WITH revenue0 AS
        (SELECT
            L_SUPPKEY AS SUPPLIER_NO,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
        FROM
            lineitem
        WHERE
            L_SHIPDATE >= '1996-01-01'
            AND L_SHIPDATE < DATEADD(month, 3, '1996-01-01')
        GROUP BY
            L_SUPPKEY)
        SELECT
            S_SUPPKEY,
            S_NAME,
            S_ADDRESS,
            S_PHONE,
            TOTAL_REVENUE
        FROM
            supplier,
            revenue0
        WHERE
            S_SUPPKEY = SUPPLIER_NO
            AND TOTAL_REVENUE = (
            SELECT
                MAX(TOTAL_REVENUE)
            FROM
                revenue0)
        ORDER BY
            S_SUPPKEY
        """

cursor.execute(query)

# Fetch all rows from the result
rows = cursor.fetchall()

# Get column names
columns = [column[0] for column in cursor.description]

# Create dataframe from rows and column names
df = pd.DataFrame(rows, columns=columns)

# Save dataframe to a csv file
df.to_csv('query_output.csv', index=False)

# Close all cursors and connections
cursor.close()
cnx.close()
