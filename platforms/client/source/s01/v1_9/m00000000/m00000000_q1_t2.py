import mysql.connector
import pandas as pd 

# Define connection details
db_details = {
    "database": "tpch",
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql"
}

# Define the query
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
    L_SHIPDATE <= '1998-09-02'
GROUP BY
    L_RETURNFLAG,
    L_LINESTATUS
ORDER BY
    L_RETURNFLAG,
    L_LINESTATUS
"""

# Create a connection
cnx = mysql.connector.connect(**db_details)

# Create DataFrame
df = pd.read_sql_query(query, con=cnx)

# Write to CSV
df.to_csv('query_output.csv', index=False)

# Close the connection
cnx.close()
