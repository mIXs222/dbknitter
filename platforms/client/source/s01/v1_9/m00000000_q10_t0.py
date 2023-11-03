import pandas as pd
import MySQLdb

# Create a connection to the database
db_conn = MySQLdb.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
cursor = db_conn.cursor()

# Specify the SQL query
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
        REVENUE, C_CUSTKEY, C_NAME, C_ACCTBAL DESC
"""

# Execute the query and fetch result into a pandas dataframe
df = pd.read_sql(query, db_conn)

# Save the results to a .csv file
df.to_csv('query_output.csv', index=False)

# Close the connection to the database
db_conn.close()
