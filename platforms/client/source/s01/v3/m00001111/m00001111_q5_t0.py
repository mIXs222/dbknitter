import mysql.connector
from pymongo import MongoClient
import pandas as pd

# MySQL Connection
mysql_cn = mysql.connector.connect(user='root', 
                                   password='my-secret-pw', 
                                   host='mysql', 
                                   database='tpch')
# MongoDb Connection
client = MongoClient("mongodb://localhost:27017/")
mdb = client['tpch']

# MySQL Query
mysql_query = """SELECT
    N_NAME,
    S_SUPPKEY,
    S_NATIONKEY,
    R_REGIONKEY
FROM
    supplier,
    nation,
    region
WHERE
    S_NATIONKEY = N_NATIONKEY
    AND N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'ASIA'"""
mysql_df = pd.read_sql(mysql_query, con=mysql_cn)

# MongoDB Query
# Aggregating data from customers, orders and lineitems collections
pipeline = [{"$lookup": {"from": "orders", "localField": "C_CUSTKEY", "foreignField": "O_CUSTKEY",
                         "as": "customer_order"}},
            {"$unwind": "$customer_order"},
            {"$lookup": {"from": "lineitem", "localField": "customer_order.O_ORDERKEY",
                         "foreignField": "L_ORDERKEY", "as": "order_lineitem"}},
            {"$unwind": "$order_lineitem"},
            {"$match": {
                "customer_order.O_ORDERDATE": {"$gte": '1990-01-01', "$lt": '1995-01-01'},
                "order_lineitem.L_EXTENDEDPRICE": {"$exists": True},
                "order_lineitem.L_DISCOUNT": {"$exists": True}}
             },
            {"$project": {"C_CUSTKEY": 1, "C_NATIONKEY": 1,  "_id": 0,
                          "REVENUE": {
                              "$multiply": ["$order_lineitem.L_EXTENDEDPRICE",
                                            {"$subtract": [1, "$order_lineitem.L_DISCOUNT"]}]}
                          }
             }]

mongo_docs = list(mdb.customer.aggregate(pipeline))
mongo_df = pd.DataFrame(mongo_docs)

# Joining MySQL data and MongoDb Data
final_df = pd.merge(mysql_df, mongo_df, left_on=['S_SUPPKEY', 'S_NATIONKEY', 'R_REGIONKEY'], 
                    right_on=['C_CUSTKEY', 'C_NATIONKEY'], how='inner')

# Groupby and order by
final_result = final_df.groupby('N_NAME')['REVENUE'].sum().reset_index()
final_result = final_result.sort_values(['REVENUE'], descending=True)

# Write result to CSV
final_result.to_csv('query_output.csv', index=False)
