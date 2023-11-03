from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://localhost:27017/")
db = client["tpch"]

orders = db['orders'].find({"O_ORDERDATE":{"$gte":"1993-07-01", "$lt":"1993-10-01"}})
result = []

for order in orders:
    lineitem = db['lineitem'].find_one({"L_ORDERKEY": order["O_ORDERKEY"], "L_COMMITDATE": {"$lt": order["L_RECEIPTDATE"]}})
    if lineitem:
        result.append((order['O_ORDERPRIORITY'], ))

df = pd.DataFrame(result, columns = ['O_ORDERPRIORITY'])
df = df.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')
df = df.sort_values(by='O_ORDERPRIORITY', ascending=True)
df.to_csv('query_output.csv', index=False)
