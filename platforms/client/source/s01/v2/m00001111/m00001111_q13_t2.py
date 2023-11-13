import csv
from pymongo import MongoClient
import pandas as pd
from pandasql import sqldf

def run_query():
    # establish a connection
    client = MongoClient('mongodb', 27017)
    db = client.tpch
    
    # fetch data from MongoDB
    customer_data = db.customer.find({})
    orders_data = db.orders.find({})

    # convert to pandas DataFrame
    customer_df = pd.DataFrame(list(customer_data))
    orders_df = pd.DataFrame(list(orders_data))

    # transform the MongoDB _id column
    customer_df = customer_df.drop(["_id"], axis=1)
    orders_df = orders_df.drop(["_id"], axis=1)

    # define SQL query
    query = '''
    SELECT
        C_COUNT,
        COUNT(*) AS CUSTDIST
    FROM
        (
        SELECT
            C_CUSTKEY,
            COUNT(O_ORDERKEY) AS C_COUNT
        FROM
            customer_df LEFT OUTER JOIN orders_df ON
            C_CUSTKEY = O_CUSTKEY
            AND O_COMMENT NOT LIKE '%pending%deposits%'
        GROUP BY
            C_CUSTKEY
        )   C_ORDERS
    GROUP BY
        C_COUNT
    ORDER BY
        CUSTDIST DESC,
        C_COUNT DESC
    '''

    # execute SQL query on a dataframe
    result = sqldf(query)

    # save result to csv
    result.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    run_query()
