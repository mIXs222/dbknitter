import csv
import pymongo
from pymongo import MongoClient

client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']
#
coll_list = ["part", "supplier", "partsupp", "nation", "region"]

results = []
for coll_name in coll_list: 
    if coll_name in db.list_collection_names():
        collection = db[coll_name]        
        if coll_name == "part":
            part_query = {"P_SIZE":15, "P_TYPE": {"$regex": ".*BRASS.*"}}
            parts = collection.find(part_query)
        elif coll_name == "supplier":
            supplier_query = {"S_NATIONKEY":{"$in": list_of_S_NATIONKEY} }
            suppliers = collection.find(supplier_query)
        elif coll_name == "partsupp":
            partsupp_query = 
            {"PS_PARTKEY":{"$in": list_of_P_PARTKEY}, "PS_SUPPKEY":{"$in": list_of_S_SUPPKEY}}
            partsupps = collection.find(partsupp_query)
        elif coll_name == "nation":
            nation_query = {"N_REGIONKEY": list_of_R_REGIONKEY}
            nations = collection.find(nation_query)
        elif coll_name == "region":
            region_query = {"R_NAME": "EUROPE"}
            regions = collection.find(region_query)

results = some_logic_to_combine_all_results(parts, suppliers, partsupps, nations, regions)

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(results)
