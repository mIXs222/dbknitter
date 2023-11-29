from pymongo import MongoClient
import csv

def main():
    client = MongoClient('mongodb', 27017)
    db = client['tpch']

    # Aggregate pipeline for MongoDB to calculate total quantity per order & filter orders > 300
    pipeline = [
        {
            '$lookup': {
                'from': 'lineitem',
                'localField': 'O_ORDERKEY',
                'foreignField': 'L_ORDERKEY',
                'as': 'items'
            }
        },
        {
            '$unwind': '$items'
        },
        {
            '$group': {
                '_id': {
                    'O_ORDERKEY': '$O_ORDERKEY',
                    'O_CUSTKEY': '$O_CUSTKEY',
                    'O_ORDERDATE': '$O_ORDERDATE',
                    'O_TOTALPRICE': '$O_TOTALPRICE'
                },
                'total_qty': {
                    '$sum': '$items.L_QUANTITY'
                }
            }
        },
        {
            '$match': {
                'total_qty': {'$gt': 300}
            }
        },
        {
            '$lookup': {
                'from': 'customer',
                'localField': '_id.O_CUSTKEY',
                'foreignField': 'C_CUSTKEY',
                'as': 'customer_info'
            }
        },
        {
            '$unwind': '$customer_info'
        },
        {
            '$project': {
                'customer_name': '$customer_info.C_NAME',
                'customer_key': '$_id.O_CUSTKEY',
                'order_key': '$_id.O_ORDERKEY',
                'order_date': '$_id.O_ORDERDATE',
                'total_price': '$_id.O_TOTALPRICE',
                'total_qty': 1,
            }
        },
        {
            '$sort': {
                'total_price': -1,
                'order_date': 1
            }
        }
    ]

    results = list(db.orders.aggregate(pipeline))

    # Writing results to a CSV file
    with open('query_output.csv', mode='w', newline='') as csvfile:
        fieldnames = ['customer_name', 'customer_key', 'order_key', 'order_date', 'total_price', 'total_qty']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for data in results:
            writer.writerow({
                'customer_name': data['customer_name'],
                'customer_key': data['customer_key'],
                'order_key': data['order_key'],
                'order_date': data['order_date'],
                'total_price': data['total_price'],
                'total_qty': data['total_qty'],
            })

if __name__ == '__main__':
    main()
