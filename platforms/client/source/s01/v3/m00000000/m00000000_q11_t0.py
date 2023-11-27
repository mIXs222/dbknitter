import mysql.connector
import pandas as pd

# Connect to the mysql server

mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# Create cursor object
mycursor = mydb.cursor()

# Execute the SQL query
mycursor.execute("""
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
""")

# Fetch all the rows into a list
rows = mycursor.fetchall()

# Create a pandas dataframe from the rows
df = pd.DataFrame(rows, columns=[i[0] for i in mycursor.description])

# Write the dataframe to a csv file
df.to_csv('query_output.csv', index=False)

# close the database connection
mydb.close()
