import pymongo
import pandas as pd
from pandas.io.json import json_normalize

client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = client["tpch"]

# Query
pipeline = [ {"$lookup": {"from": "orders", "localField": "C_CUSTKEY", "foreignField": "O_CUSTKEY", "as": "customer_orders"}}, {"$unwind": "$customer_orders"}, 
             {"$match": {"C_MKTSEGMENT": "BUILDING", "customer_orders.O_ORDERDATE":{"$lt":"1995-03-15"}}},
             {"$lookup": {"from": "lineitem", "localField": "customer_orders.O_ORDERKEY", "foreignField": "L_ORDERKEY", "as": "customer_orders_lineitem"}},
             {"$unwind": "$customer_orders_lineitem"},
             {"$match": {"customer_orders_lineitem.L_SHIPDATE": {"$gt":"1995-03-15"}}}, 
             {"$group": {"_id": { "L_ORDERKEY":"$customer_orders_lineitem.L_ORDERKEY", "O_ORDERDATE":"$customer_orders.O_ORDERDATE",
                                  "O_SHIPPRIORITY":"$customer_orders.O_SHIPPRIORITY"}, 
                         "REVENUE": { "$sum": { "$multiply": [ "$customer_orders_lineitem.L_EXTENDEDPRICE",
                                                              {"$subtract": [1, "$customer_orders_lineitem.L_DISCOUNT"]}]}}} },
             {"$sort":  { "REVENUE": -1, "_id.O_ORDERDATE":1} } ]

results = mydb['customer'].aggregate(pipeline)

# Flatten data and covert to pandas DataFrame  
df = json_normalize(list(results))

# Output
df.to_csv('query_output.csv')
