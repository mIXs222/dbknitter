import mysql.connector
import pandas as pd

# Establish a connection to the MySQL database
db_connection = mysql.connector.connect(
  host="mysql",
  user="root",
  passwd="my-secret-pw",
  database="tpch"
)

# Create a cursor object using the cursor() method
cursor = db_connection.cursor()

sql_query = """
SELECT S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT
FROM part, supplier, partsupp, nation, region
WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY AND P_SIZE = 15 AND P_TYPE LIKE '%BRASS'
AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'EUROPE'
AND PS_SUPPLYCOST = (SELECT MIN(PS_SUPPLYCOST) FROM partsupp, supplier, nation, region
WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY AND S_NATIONKEY = N_NATIONKEY
AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'EUROPE')
ORDER BY S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY
"""

# Execute the SQL command
cursor.execute(sql_query)

# Fetch all the rows in a list of lists and create a pandas dataframe
df = pd.DataFrame(cursor.fetchall())

# Set DataFrame column names
df.columns = [i[0] for i in cursor.description]

# Export the DataFrame to a csv
df.to_csv('query_output.csv', index=False)

# Close the cursor and connection
cursor.close()
db_connection.close()
