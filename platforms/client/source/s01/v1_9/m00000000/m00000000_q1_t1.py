# Import necessary libraries
import mysql.connector
import pandas as pd
from pandas import DataFrame

# Setup MySQL connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw', 
                              host='mysql', 
                              database='tpch')

# Create a cursor object
cursor = cnx.cursor()

# Define the query
query = ("""
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
        L_SHIPDATE <= '1998-09-02'
    GROUP BY
        L_RETURNFLAG,
        L_LINESTATUS
    ORDER BY
        L_RETURNFLAG,
        L_LINESTATUS
""")

# Execute the query
cursor.execute(query)

# Fetch all the rows
rows = cursor.fetchall()

# Convert the data to a pandas dataframe
df = DataFrame(rows, columns=cursor.column_names)

# Write the dataframe to a csv file
df.to_csv('query_output.csv', index=False)

# Close cursor and connection
cursor.close()
cnx.close()
