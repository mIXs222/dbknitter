import mysql.connector
import pandas as pd

# connect to the mysql server
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# cursor for executing sql
mycursor = mydb.cursor()

# Your SQL query
sql = """
SELECT
    NATION,
    YEAR(O_ORDERDATE) AS O_YEAR,
    SUM(AMOUNT) AS SUM_PROFIT
FROM
    (
    SELECT
        N_NAME AS NATION,
        O_ORDERDATE AS O_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT
    FROM
        PART,
        SUPPLIER,
        LINEITEM,
        PARTSUPP,
        ORDERS,
        NATION
    WHERE
        S_SUPPKEY = L_SUPPKEY
        AND PS_SUPPKEY = L_SUPPKEY
        AND PS_PARTKEY = L_PARTKEY
        AND P_PARTKEY = L_PARTKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND P_NAME LIKE '%dim%'
    ) AS PROFIT
GROUP BY
    NATION,
    O_YEAR
ORDER BY
    NATION,
    O_YEAR DESC
"""

df = pd.read_sql(sql, con=mydb) # execute the query and take the data in pandas dataframe
df.to_csv('query_output.csv', index=False) # write the dataframe to a csv file

# close the cursor and database connections
mycursor.close()
mydb.close()
