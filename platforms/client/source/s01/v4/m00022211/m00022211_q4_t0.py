from pymongo import MongoClient
import csv

def query_mongodb():
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    orders = db['orders']
    lineitem = db['lineitem']
    
    # Aggregation Pipeline for MongoDB
    pipeline = [
        {
            '$match': {
                'O_ORDERDATE': {
                    '$gte': '1993-07-01',
                    '$lt': '1993-10-01'
                }
            }
        },
        {
            '$lookup': {
                'from': 'lineitem',
                'localField': 'O_ORDERKEY',
                'foreignField': 'L_ORDERKEY',
                'as': 'lineitems'
            }
        },
        {
            '$match': {
                'lineitems': {
                    '$elemMatch': {
                        'L_COMMITDATE': {'$lt': '$$L_RECEIPTDATE'}
                    }
                }
            }
        },
        {
            '$group': {
                '_id': '$O_ORDERPRIORITY',
                'ORDER_COUNT': {'$sum': 1}
            }
        },
        {
            '$sort': {'_id': 1}
        }
    ]

    results = list(orders.aggregate(pipeline))
    return [(result['_id'], result['ORDER_COUNT']) for result in results]

def write_to_csv(data):
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])  # header
        for row in data:
            writer.writerow(row)

if __name__ == '__main__':
    result_data = query_mongodb()
    write_to_csv(result_data)
