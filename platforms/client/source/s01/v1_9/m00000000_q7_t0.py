import pandas as pd
import MySQLdb

# Creating a function to establish the MySQL connection
def establish_conn():
    conn = MySQLdb.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        db="tpch"
    )
    return conn

# Function to execute a query
def exec_query(query):
    df = pd.read_sql_query(
        query,
        establish_conn()
    )
    return df

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
            SUPPLIER,
            LINEITEM,
            ORDERS,
            CUSTOMER,
            NATION N1,
            NATION N2
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

# Saving the data to a CSV file.
output = exec_query(query)
output.to_csv("query_output.csv", index=False)
