import pymongo
import pandas as pd
import pandasql as psql
from pymongo import MongoClient

# connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch

# load the collections into pandas DataFrame
customer = pd.DataFrame(list(db.customer.find()))
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# drop the MongoDB specific _id column
customer.drop('_id', axis=1, inplace=True)
orders.drop('_id', axis=1, inplace=True)
lineitem.drop('_id', axis=1, inplace=True)

# MySQL Query 
sql_query = """
SELECT
    L_ORDERKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
    O_ORDERDATE,
    O_SHIPPRIORITY
FROM
    customer,
    orders,
    lineitem
WHERE
    C_MKTSEGMENT = 'BUILDING'
    AND C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE < '1995-03-15'
    AND L_SHIPDATE > '1995-03-15'
GROUP BY
    L_ORDERKEY,
    O_ORDERDATE,
    O_SHIPPRIORITY
ORDER BY
    REVENUE DESC,
    O_ORDERDATE"""

# execute the SQL query
query_output = psql.sqldf(sql_query)

# write out the output to a CSV file
query_output.to_csv('query_output.csv', index=False)

