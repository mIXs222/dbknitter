import mysql.connector
import pandas as pd

# Connect to the database
cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')

cursor = cnx.cursor()

query = """SELECT
    C_NAME,
    C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE,
    SUM(L_QUANTITY)
FROM
    CUSTOMER,
    ORDERS,
    LINEITEM
WHERE
    O_ORDERKEY IN (
    SELECT
        L_ORDERKEY
    FROM
        LINEITEM
    GROUP BY
    L_ORDERKEY HAVING
        SUM(L_QUANTITY) > 300
    )
AND C_CUSTKEY = O_CUSTKEY
AND O_ORDERKEY = L_ORDERKEY
GROUP BY
    C_NAME,
    C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE
ORDER BY
    O_TOTALPRICE DESC,
    O_ORDERDATE"""

cursor.execute(query)

# Fetch all rows from query
data = cursor.fetchall()

# Close the cursor and connection
cursor.close()
cnx.close()

# Extract the column names
columns = [i[0] for i in cursor.column_names]

df = pd.DataFrame(data, columns=columns)
# save to csv
df.to_csv('query_output.csv', index=False)
