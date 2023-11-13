# Importing necessary libraries
import pandas as pd
import mysql.connector

def fetch_data_from_mysql(query):
    """
    A function to connect to the MySQL database, execute a query and return a DataFrame
    """
    # connect to MySQL
    my_conn = mysql.connector.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        database="tpch"
    )

    # create a cursor
    cursor = my_conn.cursor()

    # execute the query
    cursor.execute(query)

    # fetch the results into a DataFrame
    df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)

    # close the connection
    my_conn.close()

    return df

# defining the query
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

# get the DataFrame
df = fetch_data_from_mysql(query)

# write to a .csv file
df.to_csv('query_output.csv', index=False)
