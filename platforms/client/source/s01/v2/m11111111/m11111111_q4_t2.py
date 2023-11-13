from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine

# Connection to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['tpch']
orders_coll = db['orders']
lineitem_coll = db['lineitem']

# Fetch data from MongoDB
orders_data = pd.DataFrame(list(orders_coll.find({}, {'_id': 0})))
lineitem_data = pd.DataFrame(list(lineitem_coll.find({}, {'_id': 0})))

# Load data into MySQL (Supposing the MySQL server is running locally at port 3306)
engine = create_engine('mysql://username:password@localhost:3306/tpch')

with engine.begin() as connection:
    orders_data.to_sql('orders', connection, if_exists='replace', index=False)
    lineitem_data.to_sql('lineitem', connection, if_exists='replace', index=False)

# Now execute the given query
with engine.begin() as connection:
    data = pd.read_sql("""
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
    """, connection)

# Export data to a CSV file
data.to_csv('query_output.csv', index=False)
