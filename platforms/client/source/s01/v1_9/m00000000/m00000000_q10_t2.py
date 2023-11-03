import mysql.connector
import pandas as pd

# function to establish connection with MySQL database
def create_db_connection(host_name, user_name, user_password, db_name):
    conn = None
    try:
        conn = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connected to Database")
    except mysql.connector.Error as error:
        print(f"Error: {error}")
    
    return conn

# function to execute query and write to csv file
def execute_query_and_write_to_csv(query, conn, file_name):
    if conn is None:
        print("No Connection to Database")
        return
    
    df = pd.read_sql_query(query, conn)
    df.to_csv(file_name, index=False)
    print(f"Data has been written to {file_name}")

# create connection
connection = create_db_connection("mysql","root","my-secret-pw","tpch")

# SQL query
query = """
SELECT
    C_CUSTKEY,
    C_NAME,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
    C_ACCTBAL,
    N_NAME,
    C_ADDRESS,
    C_PHONE,
    C_COMMENT
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE >= '1993-10-01'
    AND O_ORDERDATE < '1994-01-01'
    AND L_RETURNFLAG = 'R'
    AND C_NATIONKEY = N_NATIONKEY
GROUP BY
    C_CUSTKEY,
    C_NAME,
    C_ACCTBAL,
    C_PHONE,
    N_NAME,
    C_ADDRESS,
    C_COMMENT
ORDER BY
    REVENUE DESC, C_CUSTKEY, C_NAME, C_ACCTBAL DESC
"""

# execute query and write to csv
execute_query_and_write_to_csv(query, connection, 'query_output.csv')
