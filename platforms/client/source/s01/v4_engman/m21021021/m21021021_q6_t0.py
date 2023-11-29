from pymongo import MongoClient
import csv

# Connect to the MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Define the query
start_date = "1994-01-01"
end_date = "1995-01-01"
lower_discount_limit = 0.05
upper_discount_limit = 0.07
max_quantity = 24

# Perform the query
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gt': start_date, '$lt': end_date},
            'L_DISCOUNT': {'$gte': lower_discount_limit, '$lte': upper_discount_limit},
            'L_QUANTITY': {'$lt': max_quantity}
        }
    },
    {
        '$project': {
            'REVENUE': {'$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']}
        }
    },
    {
        '$group': {
            '_id': None,
            'TOTAL_REVENUE': {'$sum': '$REVENUE'}
        }
    }
]

result = list(lineitem_collection.aggregate(pipeline))

# Write the result to the file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['TOTAL_REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerow(result[0] if result else {'TOTAL_REVENUE': 0})

# Close the connection
client.close()
