from pymongo import MongoClient
import csv

# Function to connect to the MongoDB
def connect_to_mongodb(hostname, port, db_name):
    client = MongoClient(hostname, port)
    db = client[db_name]
    return db

def execute_query(db):
    orders_coll = db.orders
    lineitem_coll = db.lineitem

    # Looking for orders within the specified date range
    qualifying_orders = orders_coll.find({
        'O_ORDERDATE': {'$gte': '1993-07-01', '$lt': '1993-10-01'}
    }, {'O_ORDERKEY': 1, 'O_ORDERPRIORITY': 1})

    # Extract qualifying order keys
    qualifying_order_keys = [order['O_ORDERKEY'] for order in qualifying_orders]

    # Count lineitems per order where the receiptdate is later than commitdate
    pipeline = [
        {'$match': {
            'L_ORDERKEY': {'$in': qualifying_order_keys},
            'L_RECEIPTDATE': {'$gt': '$L_COMMITDATE'}
        }},
        {'$group': {
            '_id': '$L_ORDERKEY',
            'count': {'$sum': 1}
        }},
        {'$match': {
            'count': {'$gte': 1}
        }}
    ]
    qualified_lineitems = lineitem_coll.aggregate(pipeline)

    # Create a dictionary of counts per priority
    priority_counts = {}

    for lineitem in qualified_lineitems:
        order = orders_coll.find_one({'O_ORDERKEY': lineitem['_id']}, {'O_ORDERPRIORITY': 1})
        order_priority = order['O_ORDERPRIORITY']
        if order_priority in priority_counts:
            priority_counts[order_priority] += 1
        else:
            priority_counts[order_priority] = 1

    return priority_counts

if __name__ == "__main__":
    # Connect to MongoDB
    db = connect_to_mongodb('mongodb', 27017, 'tpch')
    
    # Execute the query
    order_priority_counts = execute_query(db)
    
    # Write the output to CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['ORDER_COUNT', 'O_ORDERPRIORITY']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for priority, count in sorted(order_priority_counts.items()):
            writer.writerow({'ORDER_COUNT': count, 'O_ORDERPRIORITY': priority})
