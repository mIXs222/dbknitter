import mysql.connector
import pandas as pd

# Create connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql', database='tpch')

# Create a cursor
cursor = cnx.cursor()

# Execute query
query = """
SELECT
    100.00 * SUM(CASE WHEN P_TYPE LIKE 'PROMO%' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT)
    ELSE 0
    END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE
FROM
    lineitem,
    part
WHERE
    L_PARTKEY = P_PARTKEY
    AND L_SHIPDATE >= '1995-09-01'
    AND L_SHIPDATE < '1995-10-01'
"""
cursor.execute(query)

# Get result in DataFrame
result = pd.DataFrame(cursor.fetchall(), columns=['PROMO_REVENUE'])

# Write to CSV
result.to_csv('query_output.csv', index=False)

# Close connection and cursor
cursor.close()
cnx.close()
