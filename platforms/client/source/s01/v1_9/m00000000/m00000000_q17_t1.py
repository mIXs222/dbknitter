import mysql.connector
import pandas as pd

# Define the MySQL connection parameters
db_config = {
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql",
    "database": "tpch"
}

# Connect to the MySQL server
cnx = mysql.connector.connect(**db_config)
cursor = cnx.cursor()

# Execute the query
query = """
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
"""
df = pd.read_sql_query(query, cnx)

# Write the output dataframe to a CSV file
df.to_csv('query_output.csv', index=False)

# Close the cursor and connection
cursor.close()
cnx.close()
