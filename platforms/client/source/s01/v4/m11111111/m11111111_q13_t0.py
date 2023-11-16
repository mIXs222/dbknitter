from pymongo import MongoClient
import csv
import os

# Connection to MongoDB database
client = MongoClient('mongodb', 27017)
db = client.tpch

def main():
    # MongoDB aggregation pipeline equivalent to the SQL query
    pipeline = [
        {
            "$lookup": {
                "from": "orders",
                "let": {"customer_id": "$C_CUSTKEY"},
                "pipeline": [
                    {"$match": {"$expr": {"$and": [{"$eq": ["$O_CUSTKEY", "$$customer_id"]},
                                                   {"$not": {"$regexMatch": {"input": "$O_COMMENT", "regex": "pending.*deposits"}}}]}}},
                ],
                "as": "customer_orders",
            }
        },
        {
            "$unwind": {
                "path": "$customer_orders",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$group": {
                "_id": "$C_CUSTKEY",
                "C_COUNT": {"$sum": 1}
            }
        },
        {
            "$group": {
                "_id": "$C_COUNT",
                "CUSTDIST": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "C_COUNT": "$_id",
                "CUSTDIST": 1
            }
        },
        { "$sort": {"CUSTDIST": -1, "C_COUNT": -1} }
    ]

    result = db.customer.aggregate(pipeline)

    # Write query output to query_output.csv file
    output_file_path = 'query_output.csv'
    with open(output_file_path, 'w', newline='') as csvfile:
        fieldnames = ['C_COUNT', 'CUSTDIST']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for data in result:
            writer.writerow(data)

    print(f'Query results have been written to {output_file_path}')

if __name__ == '__main__':
    main()
