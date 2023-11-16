from pymongo import MongoClient
import csv

# Establish connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch

# Get all collections
nations = db.nation.find()
regions = db.region.find()
parts = db.part.find({'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS$'}})
suppliers = db.supplier.find()
partsupps = db.partsupp.find()

# Filtering regions (in memory)
europe_regions = {doc['R_REGIONKEY'] for doc in regions if doc['R_NAME'] == 'EUROPE'}

# Filtering nations (in memory)
europe_nations = {doc['N_NATIONKEY']: doc for doc in nations if doc['N_REGIONKEY'] in europe_regions}

# Create a map of suppliers in Europe
europe_suppliers = {doc['S_SUPPKEY']: doc for doc in suppliers if doc['S_NATIONKEY'] in europe_nations}

# Consider partsupp as potential large collection, calculate MIN(PS_SUPPLYCOST) for Europe suppliers
min_supply_cost = float('inf') # Initialize with positive infinity
for partsupp_doc in partsupps:
    if partsupp_doc['PS_SUPPKEY'] in europe_suppliers and partsupp_doc['PS_SUPPLYCOST'] < min_supply_cost:
        min_supply_cost = partsupp_doc['PS_SUPPLYCOST']

# Now we filter parts and prepare the final output
output_rows = []
for part_doc in parts:
    # Get all europe partsupps for the current part
    europe_partsupps = db.partsupp.find({
        'PS_PARTKEY': part_doc['P_PARTKEY'],
        'PS_SUPPLYCOST': min_supply_cost,
        'PS_SUPPKEY': {'$in': list(europe_suppliers.keys())}
    })
    
    for partsupp_doc in europe_partsupps:
        supplier_doc = europe_suppliers[partsupp_doc['PS_SUPPKEY']]
        nation_doc = europe_nations[supplier_doc['S_NATIONKEY']]

        output_row = {
            'S_ACCTBAL': supplier_doc['S_ACCTBAL'],
            'S_NAME': supplier_doc['S_NAME'],
            'N_NAME': nation_doc['N_NAME'],
            'P_PARTKEY': part_doc['P_PARTKEY'],
            'P_MFGR': part_doc['P_MFGR'],
            'S_ADDRESS': supplier_doc['S_ADDRESS'],
            'S_PHONE': supplier_doc['S_PHONE'],
            'S_COMMENT': supplier_doc['S_COMMENT']
        }
        output_rows.append(output_row)

# Sorting the output rows by the requested fields
output_rows.sort(key=lambda x: (-x['S_ACCTBAL'], x['N_NAME'], x['S_NAME'], x['P_PARTKEY']))

# Write output to CSV file
with open('query_output.csv', 'w') as csvfile:
    fieldnames = ['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for row in output_rows:
        writer.writerow(row)
