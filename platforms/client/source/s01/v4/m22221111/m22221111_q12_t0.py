import csv
from pymongo import MongoClient
from datetime import datetime

def main():
    # MongoDB connection setup
    client = MongoClient('mongodb', 27017)
    db = client.tpch

    # Fetch orders and lineitems collections
    orders_collection = db.orders
    lineitems_collection = db.lineitem

    # Query conditions
    ship_modes = ['MAIL', 'SHIP']
    date_lower_bound = datetime.strptime('1994-01-01', '%Y-%m-%d')
    date_upper_bound = datetime.strptime('1995-01-01', '%Y-%m-%d')

    # MongoDB aggregation pipeline
    pipeline = [
        {
            '$match': {
                'L_SHIPMODE': {'$in': ship_modes},
                'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'},
                'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
                'L_RECEIPTDATE': {'$gte': date_lower_bound, '$lt': date_upper_bound}
            }
        },
        {
            '$lookup': {
                'from': 'orders',
                'localField': 'L_ORDERKEY',
                'foreignField': 'O_ORDERKEY',
                'as': 'order_docs'
            }
        },
        {'$unwind': '$order_docs'},
        {
            '$group': {
                '_id': '$L_SHIPMODE',
                'HIGH_LINE_COUNT': {
                    '$sum': {
                        '$cond': [
                            {'$or': [
                                {'$eq': ['$order_docs.O_ORDERPRIORITY', '1-URGENT']},
                                {'$eq': ['$order_docs.O_ORDERPRIORITY', '2-HIGH']}
                            ]},
                            1,
                            0
                        ]
                    }
                },
                'LOW_LINE_COUNT': {
                    '$sum': {
                        '$cond': [
                            {'$and': [
                                {'$ne': ['$order_docs.O_ORDERPRIORITY', '1-URGENT']},
                                {'$ne': ['$order_docs.O_ORDERPRIORITY', '2-HIGH']}
                            ]},
                            1,
                            0
                        ]
                    }
                }
            }
        },
        {'$sort': {'_id': 1}}  # Sorting by ship mode
    ]

    # Execute aggregation pipeline
    result = lineitems_collection.aggregate(pipeline)

    # Write results to CSV file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
        
        for row in result:
            writer.writerow([row['_id'], row['HIGH_LINE_COUNT'], row['LOW_LINE_COUNT']])

if __name__ == "__main__":
    main()
