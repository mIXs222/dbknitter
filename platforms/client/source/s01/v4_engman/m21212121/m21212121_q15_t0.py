import pymongo
import csv
import datetime

# Function to connect to MongoDB
def mongodb_connect(database_name, port, hostname):
    client = pymongo.MongoClient(hostname, port)
    db = client[database_name]
    return db

# Function to perform Top Supplier Query in MongoDB
def top_supplier_query_mongodb(db):
    # Define the date range for the query
    start_date = datetime.datetime(1996, 1, 1)
    end_date = datetime.datetime(1996, 4, 1)

    # Aggregate to calculate total revenue for each supplier and extract the required fields
    pipeline = [
        # Join supplier with lineitem collections
        {
            '$lookup': {
                'from': 'lineitem',
                'localField': 'S_SUPPKEY',
                'foreignField': 'L_SUPPKEY',
                'as': 'lineitems'
            }
        },
        # Unwind the lineitems array
        {'$unwind': '$lineitems'},
        # Filter by date range
        {
            '$match': {
                'lineitems.L_SHIPDATE': {
                    '$gte': start_date,
                    '$lt': end_date
                }
            }
        },
        # calculate the revenue
        {
            '$project': {
                'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_PHONE': 1,
                'revenue': {
                    '$multiply': [
                        '$lineitems.L_EXTENDEDPRICE',
                        {'$subtract': [1, '$lineitems.L_DISCOUNT']}
                    ]
                }
            }
        },
        # Group by supplier and sum the revenue
        {
            '$group': {
                '_id': {
                    'S_SUPPKEY': '$S_SUPPKEY',
                    'S_NAME': '$S_NAME',
                    'S_ADDRESS': '$S_ADDRESS',
                    'S_PHONE': '$S_PHONE'
                },
                'TOTAL_REVENUE': {'$sum': '$revenue'}
            }
        },
        # Sort by TOTAL_REVENUE in descending order and S_SUPPKEY in ascending order
        {'$sort': {'TOTAL_REVENUE': -1, '_id.S_SUPPKEY': 1}},
    ]

    # Execute the aggregation pipeline
    cursor = db.supplier.aggregate(pipeline)

    # Find the maximum revenue
    max_revenue = None
    for document in cursor:
        revenue = document['TOTAL_REVENUE']
        if max_revenue is None or revenue > max_revenue:
            max_revenue = revenue

    # Only keep suppliers with the maximum revenue
    top_suppliers = [doc for doc in cursor if doc['TOTAL_REVENUE'] == max_revenue]
    
    return top_suppliers

# Connect to MongoDB
db = mongodb_connect('tpch', 27017, 'mongodb')

# Execute query and get the top suppliers
top_suppliers = top_supplier_query_mongodb(db)

# Write results to CSV
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
    for supplier in top_suppliers:
        writer.writerow([supplier['_id']['S_SUPPKEY'], supplier['_id']['S_NAME'], supplier['_id']['S_ADDRESS'], supplier['_id']['S_PHONE'], supplier['TOTAL_REVENUE']])
