# import necessary libraries
from pymongo import MongoClient
import csv
from datetime import datetime

# connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# retrieve the collections
suppliers = db['supplier']
lineitems = db['lineitem']

# define start and end dates for the query
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)

# aggregate query to calculate total revenue contribution by suppliers
aggregate_query = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'total_revenue': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}}
        }
    },
    {
        '$sort': {'total_revenue': -1, '_id': 1}
    }
]

# execute the query
result = list(lineitems.aggregate(aggregate_query))

# if there is a result, find the max revenue
if result:
    max_revenue = result[0]['total_revenue']
    # filter suppliers with max revenue
    top_suppliers = [res['_id'] for res in result if res['total_revenue'] == max_revenue]
    # get suppliers info
    suppliers_info = list(suppliers.find({'S_SUPPKEY': {'$in': top_suppliers}}, {'_id': 0}))

    # write to csv
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])
        for supplier in suppliers_info:
            csvwriter.writerow([supplier['S_SUPPKEY'], supplier['S_NAME'], supplier['S_ADDRESS'],
                                supplier['S_NATIONKEY'], supplier['S_PHONE'], supplier['S_ACCTBAL'],
                                supplier['S_COMMENT']])
