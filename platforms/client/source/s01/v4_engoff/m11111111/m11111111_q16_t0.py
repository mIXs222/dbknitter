import pymongo
import csv

def connect_to_mongodb():
    # Establish connection to MongoDB
    client = pymongo.MongoClient("mongodb://mongodb:27017/")
    return client['tpch']

def get_supplier_parts(mongodb):
    valid_sizes = [49, 14, 23, 45, 19, 3, 36, 9]
    results = mongodb.part.aggregate([
        {
            # Find parts with the given attributes that do not match the excluded criteria
            "$match": {
                "P_SIZE": {"$in": valid_sizes},
                "P_TYPE": {"$ne": "MEDIUM POLISHED"},
                "P_BRAND": {"$ne": "Brand#45"}
            }
        },
        {
            # Lookup matching suppliers in partsupp
            "$lookup": {
                "from": "partsupp",
                "localField": "P_PARTKEY",
                "foreignField": "PS_PARTKEY",
                "as": "suppliers"
            }
        },
        {"$unwind": "$suppliers"},
        {
            # Lookup corresponding supplier details and filter by complaints
            "$lookup": {
                "from": "supplier",
                "let": {"suppkey": "$suppliers.PS_SUPPKEY"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$S_SUPPKEY", "$$suppkey"]
                            }
                        }
                    },
                    {
                        "$match": {
                            "S_COMMENT": {
                                "$not": {
                                    "$regex": ".*Customer.*Complaints.*"
                                }
                            }
                        }
                    },
                ],
                "as": "verified_suppliers"
            }
        },
        {"$match": {"verified_suppliers": {"$ne": []}}},
        {
            # Group parts by brand, type, and size and count distinct suppliers
            "$group": {
                "_id": {
                    "P_BRAND": "$P_BRAND",
                    "P_TYPE": "$P_TYPE",
                    "P_SIZE": "$P_SIZE"
                },
                "supplier_count": {"$sum": 1}
            }
        },
        {
            # Sort the results as required
            "$sort": {
                "supplier_count": -1,
                "_id.P_BRAND": 1,
                "_id.P_TYPE": 1,
                "_id.P_SIZE": 1
            }
        },
        {
            # Project the final structure
            "$project": {
                "_id": 0,
                "P_BRAND": "$_id.P_BRAND",
                "P_TYPE": "$_id.P_TYPE",
                "P_SIZE": "$_id.P_SIZE",
                "supplier_count": 1
            }
        }
    ])

    return list(results)

def write_output_to_csv(data, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["P_BRAND", "P_TYPE", "P_SIZE", "supplier_count"])
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def main():
    mongodb = connect_to_mongodb()
    supplier_parts_data = get_supplier_parts(mongodb)
    write_output_to_csv(supplier_parts_data, 'query_output.csv')

if __name__ == '__main__':
    main()
