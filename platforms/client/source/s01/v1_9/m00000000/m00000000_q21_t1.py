import mysql.connector
import pandas as pd

# configuration for connecting to the MySQL server
config = {
  'user': 'root',
  'password': 'my-secret-pw',
  'host': 'mysql',
  'database': 'tpch',
}

# SQL query
query = """
SELECT S_NAME, COUNT(*) AS NUMWAIT
FROM supplier, lineitem AS L1, orders, nation
WHERE S_SUPPKEY = L1.L_SUPPKEY
AND O_ORDERKEY = L1.L_ORDERKEY
AND O_ORDERSTATUS = 'F'
AND L1.L_RECEIPTDATE > L1.L_COMMITDATE
AND EXISTS (
    SELECT *
    FROM lineitem AS L2
    WHERE L2.L_ORDERKEY = L1.L_ORDERKEY
    AND L2.L_SUPPKEY <> L1.L_SUPPKEY
)
AND NOT EXISTS (
    SELECT *
    FROM lineitem AS L3
    WHERE L3.L_ORDERKEY = L1.L_ORDERKEY
    AND L3.L_SUPPKEY <> L1.L_SUPPKEY
    AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
)
AND S_NATIONKEY = N_NATIONKEY
AND N_NAME = 'SAUDI ARABIA'
GROUP BY S_NAME
ORDER BY NUMWAIT DESC, S_NAME
"""

try:
    # establishing the connection to MySQL
    conn = mysql.connector.connect(**config)
    
    # creating a new cursor
    cursor = conn.cursor()

    # executing the SQL query
    cursor.execute(query)
    
    # fetching all rows from the last executed SQL statement
    result = cursor.fetchall()
    
    # get the column names from the cursor description
    column_names = [i[0] for i in cursor.description]
    
    # convert the result into pandas DataFrame
    df = pd.DataFrame(result, columns=column_names)
    
    # export the DataFrame into a CSV file
    df.to_csv('query_output.csv', index=False)

finally:
    # close the cursor and connection
    cursor.close()
    conn.close()
