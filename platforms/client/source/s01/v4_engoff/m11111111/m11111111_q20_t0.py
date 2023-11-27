# query_code.py

from pymongo import MongoClient
import csv
from datetime import datetime

# Establishing connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Helper function to find suppliers with excess of forest part
def find_suppliers_for_forest_parts():
    suppliers_with_excess = []
    # 1. Identify all parts with a name that follows the 'forest' naming convention
    forest_parts_cursor = db['part'].find({"P_NAME": {"$regex": "forest", "$options": "i"}})
    forest_parts = {doc['P_PARTKEY']: doc for doc in forest_parts_cursor}

    # 2. Consider only the parts shipped within the specified date range and by suppliers to CANADA
    shipments_cursor = db['lineitem'].aggregate([
        {
            "$match": {
                "L_SHIPDATE": {"$gte": datetime(1994, 1, 1), "$lt": datetime(1995, 1, 1)},
                "L_PARTKEY": {"$in": list(forest_parts.keys())}
            }
        },
        {
            "$lookup": {
                "from": "supplier",
                "localField": "L_SUPPKEY",
                "foreignField": "S_SUPPKEY",
                "as": "supplier"
            }
        },
        {"$unwind": "$supplier"},
        {
            "$lookup": {
                "from": "nation",
                "localField": "supplier.S_NATIONKEY",
                "foreignField": "N_NATIONKEY",
                "as": "nation"
            }
        },
        {"$unwind": "$nation"},
        {"$match": {"nation.N_NAME": "CANADA"}},
        {
            "$group": {
                "_id": "$L_SUPPKEY",
                "total_qty": {"$sum": "$L_QUANTITY"},
                "part_keys": {"$addToSet": "$L_PARTKEY"}
            }
        }
    ])

    # 3. Compare the quantities shipped to the threshold to identify excess
    for shipment in shipments_cursor:
        total_qty = shipment['total_qty']
        parts_shipped = shipment['part_keys']
        qty_threshold = sum(db['partsupp'].find({"PS_PARTKEY": {"$in": parts_shipped}}).distinct("PS_AVAILQTY")) / 2
        if total_qty > qty_threshold:
            suppliers_with_excess.append(shipment['_id'])

    return suppliers_with_excess

# Get suppliers with excess and write to CSV file
suppliers_with_excess_ids = find_suppliers_for_forest_parts()
suppliers_with_excess_data = db['supplier'].find({"S_SUPPKEY": {"$in": suppliers_with_excess_ids}})

with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for supplier in suppliers_with_excess_data:
        writer.writerow({k: supplier[k] for k in fieldnames})

client.close()   # Close the MongoClient
