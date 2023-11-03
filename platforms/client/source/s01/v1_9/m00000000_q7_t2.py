import pandas as pd
import pymysql.cursors

# Define our connection string
conn = pymysql.connect(host='mysql',
                       user='root',
                       password='my-secret-pw',
                       db='tpch',
                       charset='utf8mb4',
                       cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cursor:
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
                    strftime('%Y', L_SHIPDATE) AS L_YEAR,
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
        # Execute the SQL command
        cursor.execute(query)
        
        # Fetch all the rows and convert to pandas dataframe
        data = pd.DataFrame(cursor.fetchall())
        
finally:
    conn.close()

# Write data to csv
data.to_csv('query_output.csv', index=False)
