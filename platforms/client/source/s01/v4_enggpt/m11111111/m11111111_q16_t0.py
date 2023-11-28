# analysis.py

from pymongo import MongoClient
import csv

def get_filtered_parts_collection(db):
    # Apply the filters to the "part" collection as per the query instructions
    parts_filtered = db.part.find({
        "P_BRAND": {"$ne": "Brand#45"},
        "P_TYPE": {"$not": {"$regex": '^MEDIUM POLISHED'}},
        "P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}
    }, {"_id": 0, "P_PARTKEY": 1, "P_BRAND": 1, "P_TYPE": 1, "P_SIZE": 1})
    return parts_filtered

def get_filtered_suppliers_collection(db):    
    # Exclude suppliers with comments containing 'Customer Complaints'
    suppliers_filtered = db.supplier.find({
        "S_COMMENT": {"$not": {"$regex": "Customer Complaints"}}
    }, {"_id": 0, "S_SUPPKEY": 1})
    return suppliers_filtered

def analyse_parts_and_suppliers():
    # Connect to the MongoDB database
    client = MongoClient('mongodb', 27017)
    db = client.tpch

    # Get the filtered collections
    parts_filtered = get_filtered_parts_collection(db)
    suppliers_filtered = get_filtered_suppliers_collection(db)

    # Process the partsupp collection to establish relationships and group data
    pipeline = [
        {"$lookup": {
            "from": 'part',
            "let": {"part_key": "$PS_PARTKEY"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$P_PARTKEY", "$$part_key"]},
                        ]
                    }
                }},
            ],
            "as": 'part_details'
        }},
        {"$unwind": "$part_details"},
        {"$lookup": {
            "from": 'supplier',
            "let": {"supp_key": "$PS_SUPPKEY"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$S_SUPPKEY", "$$supp_key"]},
                            {"$not": {"$regexMatch": {
                                "input": "$S_COMMENT",
                                "regex": "Customer Complaints"
                            }}}
                        ]
                    }
                }},
            ],
            "as": 'supplier_details'
        }},
        {"$unwind": "$supplier_details"},
        {"$group": {
            "_id": {
                "P_BRAND": "$part_details.P_BRAND",
                "P_TYPE": "$part_details.P_TYPE",
                "P_SIZE": "$part_details.P_SIZE"
            },
            "SUPPLIER_CNT": {"$sum": 1}
        }},
        {"$sort": {
            "SUPPLIER_CNT": -1,
            "_id.P_BRAND": 1,
            "_id.P_TYPE": 1,
            "_id.P_SIZE": 1
        }}
    ]

    results = list(db.partsupp.aggregate(pipeline))
    
    # Write the query output to a CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for result in results:
            writer.writerow({
                'P_BRAND': result['_id']['P_BRAND'],
                'P_TYPE': result['_id']['P_TYPE'],
                'P_SIZE': result['_id']['P_SIZE'],
                'SUPPLIER_CNT': result['SUPPLIER_CNT']
            })

if __name__ == "__main__":
    analyse_parts_and_suppliers()
