import mysql.connector
import pandas as pd

# Define a function to handle database operations
def connect_and_query():
    # Set up the database connection
    cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                                  host='mysql', database='tpch')

    # Execute the SQL Query
    cursor = cnx.cursor()
    query = ("SELECT S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT "
             "FROM part, supplier, partsupp, nation, region "
             "WHERE P_PARTKEY = PS_PARTKEY "
             "AND S_SUPPKEY = PS_SUPPKEY "
             "AND P_SIZE = 15 "
             "AND P_TYPE LIKE '%BRASS' "
             "AND S_NATIONKEY = N_NATIONKEY "
             "AND N_REGIONKEY = R_REGIONKEY "
             "AND R_NAME = 'EUROPE' "
             "AND PS_SUPPLYCOST = ("
             "SELECT MIN(PS_SUPPLYCOST) "
             "FROM partsupp, supplier, nation, region "
             "WHERE P_PARTKEY = PS_PARTKEY "
             "AND S_SUPPKEY = PS_SUPPKEY "
             "AND S_NATIONKEY = N_NATIONKEY "
             "AND N_REGIONKEY = R_REGIONKEY "
             "AND R_NAME = 'EUROPE'"
             ") "
             "ORDER BY S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY")
    cursor.execute(query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Close the cursor and connection
    cursor.close()
    cnx.close()

    # Convert the result to a DataFrame
    df = pd.DataFrame(rows, columns=['S_ACCTBAL','S_NAME','N_NAME','P_PARTKEY','P_MFGR','S_ADDRESS','S_PHONE','S_COMMENT'])

    # Write the DataFrame to a CSV file
    df.to_csv('query_output.csv', index=False)

# Connect to the database and execute the query
connect_and_query()
