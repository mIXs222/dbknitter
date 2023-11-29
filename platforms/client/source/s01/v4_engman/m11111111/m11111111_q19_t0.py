import pymongo
import csv

def get_mongodb_connection():
    client = pymongo.MongoClient('mongodb', 27017)
    return client['tpch']

def query_mongodb(db):
    match_stage = {
        "$match": {
            "$or": [
                {"$and": [
                    {"P_BRAND": "Brand#12"},
                    {"P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}},
                    {"P_SIZE": {"$gte": 1, "$lte": 5}},
                    {"L_QUANTITY": {"$gte": 1, "$lte": 11}},
                    {"L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}},
                    {"L_SHIPINSTRUCT": "DELIVER IN PERSON"}
                ]},
                {"$and": [
                    {"P_BRAND": "Brand#23"},
                    {"P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}},
                    {"P_SIZE": {"$gte": 1, "$lte": 10}},
                    {"L_QUANTITY": {"$gte": 10, "$lte": 20}},
                    {"L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}},
                    {"L_SHIPINSTRUCT": "DELIVER IN PERSON"}
                ]},
                {"$and": [
                    {"P_BRAND": "Brand#34"},
                    {"P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}},
                    {"P_SIZE": {"$gte": 1, "$lte": 15}},
                    {"L_QUANTITY": {"$gte": 20, "$lte": 30}},
                    {"L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}},
                    {"L_SHIPINSTRUCT": "DELIVER IN PERSON"}
                ]}
            ]
        }
    }
    
    project_stage = {
        "$project": {
            "_id": 0,
            "REVENUE": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}
        }
    }
    
    group_stage = {
        "$group": {
            "_id": None,
            "TOTAL_REVENUE": {"$sum": "$REVENUE"}
        }
    }
    
    pipeline = [
        {
            "$lookup": {
                "from": "part",
                "localField": "L_PARTKEY",
                "foreignField": "P_PARTKEY",
                "as": "part"
            }
        },
        {"$unwind": "$part"},
        {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$part", "$$ROOT"]}}},
        {"$project": {"part": 0}},
        match_stage,
        project_stage,
        group_stage
    ]
    
    result = list(db.lineitem.aggregate(pipeline))
    return result[0]['TOTAL_REVENUE'] if result else 0

def write_output_to_csv(output_data, filename='query_output.csv'):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['REVENUE'])
        writer.writerow([output_data])

def main():
    db = get_mongodb_connection()
    revenue = query_mongodb(db)
    write_output_to_csv(revenue)

if __name__ == "__main__":
    main()
