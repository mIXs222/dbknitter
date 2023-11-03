import pymysql
import pandas as pd

# Create a connection object
databaseServerIP = 'mysql'  # IP address of the MySQL database server
databaseUserName = 'root'  # User name of the database server
databaseUserPassword = 'my-secret-pw'  # Password for the database user
newDatabaseName = 'tpch'  # Name of the database that is to be created
charSet = 'utf8mb4'  # Character set
cusrorType = pymysql.cursors.DictCursor

connectionObject = pymysql.connect(host=databaseServerIP, 
                                   user=databaseUserName,
                                   password=databaseUserPassword,
                                   charset=charSet,
                                   cursorclass=cusrorType,
                                   db=newDatabaseName)

# Create a cursor object
cursorObject = connectionObject.cursor()

# Prepare SQL query
sqlQuery = """
with revenue0 as
(select
  L_SUPPKEY as SUPPLIER_NO,
  sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
  from
  LINEITEM
  where
  L_SHIPDATE >= '1996-01-01'
  AND L_SHIPDATE < DATE_ADD('1996-01-01', INTERVAL 3 MONTH)
  group by
  L_SUPPKEY)
select
S_SUPPKEY,
S_NAME,
S_ADDRESS,
S_PHONE,
TOTAL_REVENUE
from
SUPPLIER, 
revenue0
where
S_SUPPKEY = SUPPLIER_NO
and TOTAL_REVENUE = (
  select
  max(TOTAL_REVENUE)
  from
  revenue0
)
order by
S_SUPPKEY
"""

# Execute SQL query
df = pd.read_sql_query(sqlQuery, connectionObject)

# Write output to CSV
df.to_csv('query_output.csv', index=False)

# Close the cursor and connection
cursorObject.close()
connectionObject.close()
