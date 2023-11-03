import pymysql
import pandas as pd

# Create a connection to your database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

def run_query(query):
    return pd.read_sql_query(query, connection)

query = """
    SELECT
        CNTRYCODE,
        COUNT(*) AS NUMCUST,
        SUM(C_ACCTBAL) AS TOTACCTBAL
    FROM
        (
        SELECT
            SUBSTR(C_PHONE, 1 , 2) AS CNTRYCODE,
            C_ACCTBAL
        FROM
            customer
        WHERE
            SUBSTR(C_PHONE , 1 , 2) IN
            ('20', '40', '22', '30', '39', '42', '21')
        AND C_ACCTBAL > (
                SELECT
                    AVG(C_ACCTBAL)
                FROM
                    customer
                WHERE
                    C_ACCTBAL > 0.00
                AND SUBSTR(C_PHONE , 1 , 2) IN
                ('20', '40', '22', '30', '39', '42', '21')
        )
        AND NOT EXISTS (
            SELECT
                *
            FROM
                orders
            WHERE
                O_CUSTKEY = C_CUSTKEY
            )
        ) AS CUSTSALE
    GROUP BY
        CNTRYCODE
    ORDER BY
        CNTRYCODE
"""

# Run the query and save the results to a dataframe
df = run_query(query)

# Write the results to a csv file
df.to_csv('query_output.csv', index=False)
