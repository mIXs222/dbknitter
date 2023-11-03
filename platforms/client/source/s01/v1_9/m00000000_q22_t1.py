import mysql.connector
import pandas as pd

def fetch_data(database, username, password, host):
    mydb = mysql.connector.connect(
    host=host,
    user=username,
    passwd=password,
    database=database
    )

    mycursor = mydb.cursor()

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
            CUSTOMER
        WHERE
            SUBSTR(C_PHONE , 1 , 2) IN
            ('20', '40', '22', '30', '39', '42', '21')
        AND C_ACCTBAL > (
                SELECT
                    AVG(C_ACCTBAL)
                FROM
                    CUSTOMER
                WHERE
                    C_ACCTBAL > 0.00
                AND SUBSTR(C_PHONE , 1 , 2) IN
                ('20', '40', '22', '30', '39', '42', '21')
        )
        AND NOT EXISTS (
            SELECT
                *
            FROM
                ORDERS
            WHERE
                O_CUSTKEY = CUSTOMER.C_CUSTKEY
            )
        ) AS CUSTSALE
    GROUP BY
        CNTRYCODE
    ORDER BY
        CNTRYCODE
    """

    df = pd.read_sql(query, con=mydb)
    return df

if __name__ == "__main__":
    database = "tpch"
    username = "root"
    password = "my-secret-pw"
    host = "mysql"

    df = fetch_data(database, username, password, host)
    df.to_csv('query_output.csv', index=False)
