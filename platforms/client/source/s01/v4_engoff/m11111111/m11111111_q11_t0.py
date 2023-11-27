import pymongo
import csv

# Establish connection to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Get relevant collections from MongoDB
nation_col = db["nation"]
supplier_col = db["supplier"]
partsupp_col = db["partsupp"]

# Find the nation key for Germany
germany_nationkey = nation_col.find_one({"N_NAME": "GERMANY"})["N_NATIONKEY"]

# Find all suppliers from Germany
germany_suppliers = list(supplier_col.find({"S_NATIONKEY": germany_nationkey}))

# Calculate total value of all available parts and find significant parts
total_value = 0
part_values = {}

for supplier in germany_suppliers:
    suppkey = supplier["S_SUPPKEY"]
    partsupps = partsupp_col.find({"PS_SUPPKEY": suppkey})
    
    for partsupp in partsupps:
        partkey = partsupp["PS_PARTKEY"]
        value = partsupp["PS_AVAILQTY"] * partsupp["PS_SUPPLYCOST"]
        total_value += value
        if partkey not in part_values:
            part_values[partkey] = 0
        part_values[partkey] += value

# Filter out the significant parts
significant_parts = [(k, v) for k, v in part_values.items() if v / total_value > 0.0001]

# Sort the significant parts in descending order of value
significant_parts.sort(key=lambda x: x[1], reverse=True)

# Write results to a CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['PARTKEY', 'VALUE'])
    for part in significant_parts:
        writer.writerow(part)

# Close the connection to MongoDB
client.close()
