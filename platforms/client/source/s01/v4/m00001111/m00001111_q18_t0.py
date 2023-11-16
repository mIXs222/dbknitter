import pymongo
import csv

def mongo_query():
    # Connect to the MongoDB server
    client = pymongo.MongoClient("mongodb", 27017)
    db = client["tpch"]

    # Perform an aggregate query equivalent to the SQL provided
    pipeline = [
        # Join orders and lineitem collections
        {
            '$lookup': {
                'from': 'lineitem',
                'localField': 'O_ORDERKEY',
                'foreignField': 'L_ORDERKEY',
                'as': 'lineitems'
            }
        },
        # Unwind the lineitems after join
        {'$unwind': '$lineitems'},
        # Group by the necessary fields and sum the quantities
        {
            '$group': {
                '_id': {
                    'C_CUSTKEY': '$O_CUSTKEY',
                    'C_NAME': '$C_NAME',
                    'O_ORDERKEY': '$O_ORDERKEY',
                    'O_ORDERDATE': '$O_ORDERDATE',
                    'O_TOTALPRICE': '$O_TOTALPRICE'
                },
                'SUM_L_QUANTITY': {'$sum': '$lineitems.L_QUANTITY'}
            }
        },
        # Filter groups that have SUM_L_QUANTITY greater than 300
        {'$match': {'SUM_L_QUANTITY': {'$gt': 300}}},
        # Prepare the format similar to the SQL SELECT
        {
            '$project': {
                '_id': 0,
                'C_NAME': '$_id.C_NAME',
                'C_CUSTKEY': '$_id.C_CUSTKEY',
                'O_ORDERKEY': '$_id.O_ORDERKEY',
                'O_ORDERDATE': '$_id.O_ORDERDATE',
                'O_TOTALPRICE': '$_id.O_TOTALPRICE',
                'SUM_L_QUANTITY': 1
            }
        },
        # Sort based on the conditions
        {'$sort': {'O_TOTALPRICE': -1, 'O_ORDERDATE': 1}},
    ]

    return list(db.orders.aggregate(pipeline))


def write_to_csv(data):
    # Define the output file
    output_file = 'query_output.csv'
    fieldnames = ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM_L_QUANTITY']

    # Write the data to a CSV file
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


if __name__ == "__main__":
    query_results = mongo_query()
    write_to_csv(query_results)
