import pymysql
import csv

# Database connection details
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connect to the MySQL database
mysql_conn = pymysql.connect(**mysql_conn_info)
cursor = mysql_conn.cursor()

# SQL query to execute
mysql_query = """
SELECT
  s.S_ACCTBAL, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_COMMENT,
  p.P_PARTKEY, p.P_MFGR, p.P_SIZE,
  n.N_NAME
FROM
  supplier AS s
  JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY
  JOIN region AS r ON n.N_REGIONKEY = r.R_REGIONKEY
  JOIN partsupp AS ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
  JOIN part AS p ON ps.PS_PARTKEY = p.P_PARTKEY
WHERE
  r.R_NAME = 'EUROPE'
  AND p.P_SIZE = 15
  AND p.P_TYPE LIKE '%BRASS'
  AND ps.PS_SUPPLYCOST = (
    SELECT MIN(PS_SUPPLYCOST)
    FROM partsupp, supplier, nation, region
    WHERE partsupp.PS_SUPPKEY = supplier.S_SUPPKEY
      AND supplier.S_NATIONKEY = nation.N_NATIONKEY
      AND nation.N_REGIONKEY = region.R_REGIONKEY
      AND region.R_NAME = 'EUROPE'
  )
ORDER BY 
  s.S_ACCTBAL DESC,
  n.N_NAME,
  s.S_NAME,
  p.P_PARTKEY
"""

# Execute the query and fetch the data
cursor.execute(mysql_query)
data = cursor.fetchall()

# Write data to csv file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write the headers to the csv file
    writer.writerow(['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'N_NAME'])
    # Write the data to the csv file
    for row in data:
        writer.writerow(row)

# Close the cursor and the connection
cursor.close()
mysql_conn.close()
