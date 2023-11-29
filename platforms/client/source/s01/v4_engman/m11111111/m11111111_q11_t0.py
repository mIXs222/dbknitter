# important_stock_query.py

from pymongo import MongoClient
import csv

# MongoDB connection setup
client = MongoClient('mongodb', 27017)
db = client.tpch

def get_important_stock():
    # Find Germany's nation key
    germany_nation = db.nation.find_one({'N_NAME': 'GERMANY'}, {'N_NATIONKEY': 1})
    
    if not germany_nation:
        return []
    
    germany_nation_key = germany_nation['N_NATIONKEY']
    
    # Find suppliers from Germany
    germany_suppliers = list(db.supplier.find({'S_NATIONKEY': germany_nation_key}, {'S_SUPPKEY': 1}))
    
    if not germany_suppliers:
        return []
    
    supplier_keys = [supplier['S_SUPPKEY'] for supplier in germany_suppliers]
    
    # Find all parts supplied by those suppliers and calculate total value
    part_values = {}
    total_value = 0
    for supplier_key in supplier_keys:
        parts = db.partsupp.find({'PS_SUPPKEY': supplier_key})
        for part in parts:
            part_key = part['PS_PARTKEY']
            value = part['PS_AVAILQTY'] * part['PS_SUPPLYCOST']
            part_values[part_key] = part_values.get(part_key, 0) + value
            total_value += value
    
    # Filter parts that represent a significant percentage
    important_parts = [(part_key, value) for part_key, value in part_values.items() if value > total_value * 0.0001]

    # Sort by value in descending order
    important_parts.sort(key=lambda x: x[1], reverse=True)
    
    return important_parts

def write_to_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Part Number', 'Value'])
        for row in data:
            writer.writerow(row)

def main():
    important_parts = get_important_stock()
    write_to_csv(important_parts, 'query_output.csv')

if __name__ == '__main__':
    main()
