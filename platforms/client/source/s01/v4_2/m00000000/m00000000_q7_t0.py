import pandas as pd
import pymysql

# Establish the connection
conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    db='tpch'
)

# Create cursor
cursor = conn.cursor()

# SQL query
query = """
SELECT
    SUPP_NATION,
    CUST_NATION,
    L_YEAR,
    SUM(VOLUME) AS REVENUE
FROM
    (
    SELECT
        N1.N_NAME AS SUPP_NATION,
        N2.N_NAME AS CUST_NATION,
        YEAR(L_SHIPDATE) AS L_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME
    FROM
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
    WHERE
        S_SUPPKEY = L_SUPPKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND C_CUSTKEY = O_CUSTKEY
        AND S_NATIONKEY = N1.N_NATIONKEY
        AND C_NATIONKEY = N2.N_NATIONKEY
        AND (
            (N1.N_NAME = 'JAPAN' AND N2.N_NAME = 'INDIA')
            OR (N1.N_NAME = 'INDIA' AND N2.N_NAME = 'JAPAN')
            )
        AND L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
    ) AS SHIPPING
GROUP BY
    SUPP_NATION,
    CUST_NATION,
    L_YEAR
ORDER BY
    SUPP_NATION,
    CUST_NATION,
    L_YEAR
"""

# Execute the query
cursor.execute(query)

# Fetch all the rows
rows = cursor.fetchall()

# Create a DataFrame from rows
df = pd.DataFrame(list(rows), columns=['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'REVENUE'])

# Write to a CSV file
df.to_csv('query_output.csv', index=False)

# Close the connection
conn.close()
