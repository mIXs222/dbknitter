import pymongo
import csv
import datetime

# MongoDB connection parameters
mongodb_host = 'mongodb'
mongodb_port = 27017
mongodb_dbname = 'tpch'

# Establish connection to MongoDB
client = pymongo.MongoClient(host=mongodb_host, port=mongodb_port)
db = client[mongodb_dbname]

# Define the country codes of interest
country_codes = ['20', '40', '22', '30', '39', '42', '21']

# Calculate the date 7 years ago from today
seven_years_ago = datetime.datetime.now() - datetime.timedelta(days=7*365)

# Query to get the customers who haven't placed orders in the last 7 years
customers = db.customer.aggregate([
    {
        '$match': {
            'C_PHONE': {'$regex': '^(20|40|22|30|39|42|21)'},
            'C_ACCTBAL': {'$gt': 0.00}
        }
    },
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'C_CUSTKEY',
            'foreignField': 'O_CUSTKEY',
            'as': 'orders'
        }
    },
    {
        '$project': {
            'C_CUSTKEY': 1,
            'C_ACCTBAL': 1,
            'recentOrder': {
                '$max': '$orders.O_ORDERDATE'
            }
        }
    },
    {
        '$match': {
            'recentOrder': {'$lt': seven_years_ago}
        }
    },
    {
        '$group': {
            '_id': {'country_code': {'$substr': ['$C_PHONE', 0, 2]}},
            'customer_count': {'$sum': 1},
            'average_balance': {'$avg': '$C_ACCTBAL'}
        }
    }
])

# Exporting results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write the header
    writer.writerow(['CountryCode', 'CustomerCount', 'AverageBalance'])
    
    # Write each record
    for record in customers:
        writer.writerow([record['_id']['country_code'], record['customer_count'], record['average_balance']])
