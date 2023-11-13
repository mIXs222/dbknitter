import mysql.connector
import pandas as pd

def execute_query():

    # establishing the connection with the mysql server
    conn = mysql.connector.connect(user='root', password='my-secret-pw', 
                                   host='mysql', database='tpch')

    # preparing a cursor object
    cursor = conn.cursor()

    # defining the query
    query = """
            SELECT
                PS_PARTKEY,
                SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE
            FROM
                partsupp,
                supplier,
                nation
            WHERE
                PS_SUPPKEY = S_SUPPKEY
                AND S_NATIONKEY = N_NATIONKEY
                AND N_NAME = 'GERMANY'
            GROUP BY
                PS_PARTKEY HAVING
                SUM(PS_SUPPLYCOST * PS_AVAILQTY) >
                (
                SELECT
                    SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000
                FROM
                    partsupp,
                    supplier,
                    nation
                WHERE
                    PS_SUPPKEY = S_SUPPKEY
                    AND S_NATIONKEY = N_NATIONKEY
                    AND N_NAME = 'GERMANY'
                )
            ORDER BY
                VALUE DESC
            """
    
    df = pd.read_sql_query(query, conn)
    df.to_csv('query_output.csv', index = False)
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    execute_query()
