from pymongo import MongoClient
from operator import itemgetter
import csv
import datetime

client = MongoClient('mongodb', 27017, username='tpch') # add password as additional field if necessary
db = client['tpch']

def get_ntp_result():
    order_result = []
    for obj in list(db.customer.find({"C_MKTSEGMENT": "BUILDING"})):
        custkey = obj['C_CUSTKEY']
        for order in list(db.orders.find({"O_CUSTKEY": custkey, "O_ORDERDATE": {"$lt": datetime.datetime.strptime('1995-03-15', "%Y-%m-%d")}})):
            orderkey = order['O_ORDERKEY']
            for lineitem in list(db.lineitem.find({"L_ORDERKEY": orderkey, "L_SHIPDATE": {"$gt": datetime.datetime.strptime('1995-03-15', "%Y-%m-%d")}})):
                revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
                order_result.append((lineitem['L_ORDERKEY'], revenue, order['O_ORDERDATE'], order['O_SHIPPRIORITY']))
    return sorted(order_result, key=itemgetter(1, 2), reverse=True)

result = get_ntp_result()

with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["L_ORDERKEY", "REVENUE", "O_ORDERDATE", "O_SHIPPRIORITY"])
    for res in result:
        writer.writerow(res)
