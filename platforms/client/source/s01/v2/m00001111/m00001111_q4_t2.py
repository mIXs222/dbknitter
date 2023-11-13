from pymongo import MongoClient
import pandas as pd
from pandasql import sqldf

# function to execute SQL query
pysqldf = lambda q: sqldf(q, globals())

# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["tpch"]

# convert MongoDB collection to DataFrame
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# Define the SQL query
query = """
        SELECT
            O_ORDERPRIORITY,
            COUNT(*) AS ORDER_COUNT
        FROM
            orders
        WHERE
            O_ORDERDATE >= '1993-07-01'
            AND O_ORDERDATE < '1993-10-01'
            AND EXISTS (
                SELECT
                    *
                FROM
                    lineitem
                WHERE
                    L_ORDERKEY = O_ORDERKEY
                    AND L_COMMITDATE < L_RECEIPTDATE
                )
        GROUP BY
            O_ORDERPRIORITY
        ORDER BY
            O_ORDERPRIORITY
        """

# Execute the SQL query
result = pysqldf(query)

# Write to CSV file
result.to_csv('query_output.csv', index=False)
