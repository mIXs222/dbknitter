import mysql.connector
import pandas as pd

# Establish connection to the database
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# Prepare cursor
mycursor = mydb.cursor()

# Prepare SQL query
sql = """
SELECT
    P_BRAND,
    P_TYPE,
    P_SIZE,
    COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM
    partsupp,
    part
WHERE
    P_PARTKEY = PS_PARTKEY
    AND P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND PS_SUPPKEY NOT IN (
        SELECT
            S_SUPPKEY
        FROM
            supplier
        WHERE
            S_COMMENT LIKE '%Customer%Complaints%'
    )
GROUP BY
    P_BRAND,
    P_TYPE,
    P_SIZE
ORDER BY
    SUPPLIER_CNT DESC,
    P_BRAND,
    P_TYPE,
    P_SIZE
"""

# Execute the query
mycursor.execute(sql)

# Fetch all the rows
rows = mycursor.fetchall()

# Convert the rows to DataFrame
df = pd.DataFrame(rows, columns=['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])

# Write DataFrame to CSV
df.to_csv('query_output.csv', index=False)

# Close the cursor and database
mycursor.close()
mydb.close()
