from pymongo import MongoClient
import csv

# Constants
MONGO_DB_NAME = "tpch"
MONGO_COLLECTION_NAME = "lineitem"
MONGO_PORT = 27017
MONGO_HOSTNAME = "mongodb"

def query_mongodb():
    # Create a MongoDB client
    client = MongoClient(MONGO_HOSTNAME, MONGO_PORT)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    
    # Define the filter for the query
    filter_query = {
        "L_SHIPDATE": {
            "$gte": "1994-01-01",
            "$lte": "1994-12-31"
        },
        "L_DISCOUNT": {
            "$gte": 0.05, # 5% Lower bound (inclusive)
            "$lte": 0.07  # 7% Upper bound (inclusive)
        },
        "L_QUANTITY": {
            "$lt": 24
        }
    }
    
    # Define the projection to calculate extended price * (1 - discount)
    projection = {
        "$project": {
            "revenue": {
                "$multiply": [
                    "$L_EXTENDEDPRICE",
                    {"$subtract": [1, "$L_DISCOUNT"]}
                ]
            }
        }
    }
    
    # Define the aggregation pipeline
    pipeline = [
        {"$match": filter_query},
        projection,
        {
            "$group": {
                "_id": None,
                "total_revenue": {"$sum": "$revenue"}
            }
        }
    ]
    
    # Execute the aggregation pipeline
    cursor = collection.aggregate(pipeline)
    
    # Get the result
    result = list(cursor)
    total_revenue = result[0]['total_revenue'] if result else 0

    return total_revenue

def write_to_csv(output_filename, total_revenue):
    with open(output_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['total_revenue'])
        writer.writerow([total_revenue])

def main():
    total_revenue = query_mongodb()
    write_to_csv('query_output.csv', total_revenue)

if __name__ == "__main__":
    main()
