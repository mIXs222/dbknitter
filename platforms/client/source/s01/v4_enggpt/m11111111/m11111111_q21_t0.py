import pymongo
import csv
from datetime import datetime

# Connect to the MongoDB server
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Function that pulls data from MongoDB and performs in-depth analysis
def conduct_analysis():
    # Fetching required data from collections
    nations = list(mongo_db.nation.find({"N_NAME": "SAUDI ARABIA"}, {"_id": 0}))
    suppliers = list(mongo_db.supplier.find({"S_NATIONKEY": {"$in": [n["N_NATIONKEY"] for n in nations]}}, {"_id": 0}))
    orders = list(mongo_db.orders.find({"O_ORDERSTATUS": "F"}, {"_id": 0}))
    lineitems = list(mongo_db.lineitem.find({"L_RECEIPTDATE": {"$gt": datetime(1900, 1, 1)}, 
                                              "L_COMMITDATE": {"$gt": datetime(1900, 1, 1)}}, {"_id": 0}))

    # Creating analysis result list
    results = []
    for s in suppliers:
        for o in orders:
            for l in lineitems:
                if l["L_SUPPKEY"] == s["S_SUPPKEY"] and l["L_ORDERKEY"] == o["O_ORDERKEY"] and l["L_RECEIPTDATE"] > l["L_COMMITDATE"]:
                    other_lineitems = [li for li in lineitems if li["L_ORDERKEY"] == o["O_ORDERKEY"] and li["L_SUPPKEY"] != s["S_SUPPKEY"]]
                    if any(li["L_RECEIPTDATE"] <= l["L_COMMITDATE"] for li in other_lineitems):
                        continue
                    wait_time = (l["L_RECEIPTDATE"] - l["L_COMMITDATE"]).days
                    results.append((s["S_NAME"], wait_time))

    # Sort results based on the waiting time(descending) and supplier name(ascending)
    results.sort(key=lambda x: (-x[1], x[0]))

    # Write to CSV file
    with open('query_output.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['S_NAME', 'NUMWAIT'])
        for res in results:
            writer.writerow(res)

if __name__ == '__main__':
    conduct_analysis()
