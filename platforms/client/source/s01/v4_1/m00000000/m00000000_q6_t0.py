import csv
import pandas as pd
import pymysql

# connect to MySQL database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch')

# create cursor object
cursor = connection.cursor()

# execute SQL query
cursor.execute("""
    SELECT
        SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= '1994-01-01'
        AND L_SHIPDATE < '1995-01-01'
        AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
        AND L_QUANTITY < 24
""")

# fetch all rows from the last executed SQL statement
rows = cursor.fetchall()

# create pandas dataframe
df = pd.DataFrame(rows, columns=['REVENUE'])

# write to csv
df.to_csv('query_output.csv', index=False)

# close DB connection
connection.close()
