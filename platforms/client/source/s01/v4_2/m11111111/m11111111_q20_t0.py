import csv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

client = MongoClient('mongodb', 27017)

try:
    client.admin.command('ismaster')
except ConnectionFailure:
    print('Server not available')

db = client['tpch']

def write_to_csv(data):
    with open('query_output.csv', 'w', encoding='utf8', newline='') as output_file:
        fc = csv.DictWriter(output_file, 
                            fieldnames=data[0].keys(),
                            )
        fc.writeheader()
        fc.writerows(data)

# subquery for part
parts = list(db.part.find({'P_NAME': {'$regex': "forest"}}))

# subquery for lineitem
lineitems = list(db.lineitem.aggregate([
    {"$match": {"L_SHIPDATE": {"$gte": '1994-01-01', "$lt": '1995-01-01'}}},
    {"$group": 
        {"_id": {"partkey": "$L_PARTKEY", "suppkey": "$L_SUPPKEY"}, 
         "qty": {"$sum": "$L_QUANTITY"}}}
]))

# main query for supplier and nation
suppliers = list(db.supplier.find({"S_NATIONKEY": {"$eq": db.nation.find_one({"N_NAME": "CANADA"})["N_NATIONKEY"]}}))

# filter results
results = []
for supplier in suppliers:
    for part in parts:
        for lineitem in lineitems:
            if supplier["S_SUPPKEY"] in [p['PS_SUPPKEY'] for p in db.partsupp.find({"PS_PARTKEY": part['P_PARTKEY'], "PS_AVAILQTY": {"$gt": lineitem["qty"]*0.5}})]:
                results.append({"S_NAME": supplier["S_NAME"], "S_ADDRESS": supplier["S_ADDRESS"] })

results.sort(key=lambda x: x["S_NAME"])
write_to_csv(results)
