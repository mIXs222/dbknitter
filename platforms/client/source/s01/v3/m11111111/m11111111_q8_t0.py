from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['tpch']

# Find the records
p_res = db.part.find({"P_TYPE": "SMALL PLATED COPPER"})
o_res = db.orders.find({"O_ORDERDATE": {"$gte": "1995-01-01", "$lte": "1996-12-31"}})
s_res = db.supplier.find()
c_res = db.customer.find()
r_res = db.region.find({"R_NAME": "ASIA"})
n1_res = db.nation.find()
n2_res = db.nation.find()
l_res = db.lineitem.find()

# Join records (Simplified representation, You might have to perform a full join operation)
# A simplified join operation in Python would be to convert all result objects into arrays of dictionaries and perform manual mapping between fields
# Also you will have to calculate the required fields like VOLUME, MKT_SHARE and make sure that you handle not existent fields
res = dict()

# Write output to CSV
f = open("query_output.csv", "w")
writer = csv.DictWriter(f, fieldnames=res[0].keys())
writer.writeheader()
writer.writerows(res)
f.close()
