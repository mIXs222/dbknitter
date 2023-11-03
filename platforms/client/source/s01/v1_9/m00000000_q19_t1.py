import pandas as pd
import pymysql

# Establish connection to the MySQL database
connection = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

# Define the SQL query
sql = """
SELECT SUM(L_EXTENDEDPRICE*(1 - L_DISCOUNT)) AS REVENUE
FROM LINEITEM, PART
WHERE (
P_PARTKEY = L_PARTKEY
AND P_BRAND = 'Brand#12'
AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
AND L_QUANTITY >= 1 AND L_QUANTITY <= 1 + 10
AND P_SIZE BETWEEN 1 AND 5
AND L_SHIPMODE IN ('AIR', 'AIR REG')
AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
)
OR
(
P_PARTKEY = L_PARTKEY
AND P_BRAND = 'Brand#23'
AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
AND L_QUANTITY >= 10 AND L_QUANTITY <= 10 + 10
AND P_SIZE BETWEEN 1 AND 10
AND L_SHIPMODE IN ('AIR', 'AIR REG')
AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
)
OR
(
P_PARTKEY = L_PARTKEY
AND P_BRAND = 'Brand#34'
AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
AND L_QUANTITY >= 20 AND L_QUANTITY <= 20 + 10
AND P_SIZE BETWEEN 1 AND 15
AND L_SHIPMODE IN ('AIR', 'AIR REG')
AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
)
"""

# Execute the query and save the result to a pandas DataFrame
df = pd.read_sql(sql, connection)

# Write the result to a CSV file
df.to_csv("query_output.csv", index=False)

# Close the database connection
connection.close()
