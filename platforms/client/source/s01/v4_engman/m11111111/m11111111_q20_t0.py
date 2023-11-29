from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the query parameters
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
nation_name = "CANADA"
part_pattern = "forest"
excess_threshold = 0.5

# Fetch relevant Nation Key
nation = db.nation.find_one({"N_NAME": nation_name}, {"N_NATIONKEY": 1})
nation_key = nation['N_NATIONKEY'] if nation else None

# Find relevant suppliers based on the nation key
suppliers = list(db.supplier.find({"S_NATIONKEY": nation_key}, {"S_SUPPKEY": 1}))

# Dictionary to store suppliers and their corresponding total shipped quantities
suppliers_shipped_qty = {}

# Process each supplier to find relevant parts and quantities shipped
for supplier in suppliers:
    suppkey = supplier['S_SUPPKEY']
    part_supplies = db.partsupp.find({"PS_SUPPKEY": suppkey}, {"PS_PARTKEY": 1, "PS_SUPPKEY": 1})
    
    # Find all lineitems for this supplier within the date range
    for part_supply in part_supplies:
        partkey = part_supply['PS_PARTKEY']
        lineitems = db.lineitem.find({
            "L_SUPPKEY": suppkey,
            "L_PARTKEY": partkey,
            "L_SHIPDATE": {"$gte": start_date, "$lt": end_date},
            "L_RETURNFLAG": {"$ne": "R"}  # Exclude returned items
        })
        
        # Calculate the total quantity shipped for each part
        total_qty = sum(item['L_QUANTITY'] for item in lineitems)
        if suppkey in suppliers_shipped_qty:
            suppliers_shipped_qty[suppkey] += total_qty
        else:
            suppliers_shipped_qty[suppkey] = total_qty

# Fetch parts that match the naming convention
matching_parts = db.part.find({"P_NAME": {"$regex": part_pattern}}, {"P_PARTKEY": 1})
matching_part_keys = [part['P_PARTKEY'] for part in matching_parts]

# Find suppliers that exceed the excess threshold
excess_suppliers = {}
for suppkey, total_qty in suppliers_shipped_qty.items():
    parts = db.partsupp.find({"PS_SUPPKEY": suppkey, "PS_PARTKEY": {"$in": matching_part_keys}})
    matched_qty = sum(part['PS_AVAILQTY'] for part in parts)
    
    # Check if the supplier's matched quantity exceeds the defined threshold
    if matched_qty > total_qty * excess_threshold:
        excess_suppliers[suppkey] = {"total_quantity": total_qty, "excess_quantity": matched_qty}

# Write query results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['supplier_key', 'total_quantity', 'excess_quantity']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for suppkey, quantities in excess_suppliers.items():
        writer.writerow({'supplier_key': suppkey, **quantities})

client.close()
