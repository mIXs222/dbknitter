from pymongo import MongoClient
import csv

def connect_to_mongodb(host, port, db_name):
    client = MongoClient(host, port)
    db = client[db_name]
    return db

def aggregate_data(db):
    pipeline = [
        {
            '$lookup': {
                'from': 'orders',
                'localField': 'C_CUSTKEY',
                'foreignField': 'O_CUSTKEY',
                'as': 'orders'
            }
        },
        {
            '$unwind': {
                'path': '$orders',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$match': {
                'orders.O_COMMENT': {'$not': {'$regex': 'pending.*deposits'}}
            }
        },
        {
            '$group': {
                '_id': '$C_CUSTKEY',
                'C_COUNT': {'$sum': {'$cond': [{'$eq': ['$orders.O_ORDERKEY', None]}, 0, 1]}}
            }
        },
        {
            '$sort': {'_id': 1}
        }
    ]
    return list(db.customer.aggregate(pipeline))

def main():
    mongodb_info = {
        'host': 'mongodb',
        'port': 27017,
        'db_name': 'tpch'
    }

    db = connect_to_mongodb(**mongodb_info)
    agg_data = aggregate_data(db)
    cust_count = {}
    for data in agg_data:
        cust_count[data['C_COUNT']] = cust_count.get(data['C_COUNT'], 0) + 1

    sorted_cust_count = sorted(cust_count.items(), key=lambda x: (-x[1], -x[0]))

    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['C_COUNT', 'CUSTDIST']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for c_count, custdist in sorted_cust_count:
            writer.writerow({'C_COUNT': c_count, 'CUSTDIST': custdist})

main()
