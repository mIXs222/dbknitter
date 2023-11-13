import os
import sys
import csv
from pymongo import MongoClient
from bson.code import Code
from bson.json_util import dumps


def export_to_csv(data, filename="query_output.csv"):
    keys = data[0].keys()
    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)


# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient("mongodb://mongodb:27017/")
db = client.tpch


mapfunc = Code("function() {"
               "emit(this.C_CUSTKEY, {"
               "count: 1,"
               "orders: this.O_ORDERCOMMENT.includes('pendingdeposits') ? 0 : 1"
               "});"
               "}")

reducefunc = Code("function(keyCustId, values) {"
                  "var result = {count: 0, orders: 0};"
                  "values.forEach(function(value) {"
                  "result.count += value.count;"
                  "result.orders += value.orders;"
                  "});"
                  "return result;"
                  "}")

query = db.customers.map_reduce(mapfunc, reducefunc, "myresults")

result_query = list(query.find())

result = []
for item in result_query:
    cust_dist = {"C_COUNT": item["_id"], "CUSTDIST": item["value"]["count"]}
    result.append(cust_dist)

export_to_csv(result)
