import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cur = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']
customer_collection = mongo_db['customer']
lineitem_collection = mongo_db['lineitem']

# Query to get dictionary of nation keys and names from MySQL
nation_query = "SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'INDIA' OR N_NAME = 'JAPAN'"
mysql_cur.execute(nation_query)
results = mysql_cur.fetchall()
nation_dict = {key: name for key, name in results}

# Construct the MongoDB pipeline to perform the aggregation
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': datetime(1995, 1, 1), '$lte': datetime(1996, 12, 31)}
        }
    },
    {
        '$lookup': {
            'from': 'customer',
            'localField': 'L_CUSTKEY',
            'foreignField': 'C_CUSTKEY',
            'as': 'customer'
        }
    },
    {
        '$lookup': {
            'from': 'supplier',
            'localField': 'L_SUPPKEY',
            'foreignField': 'S_SUPPKEY',
            'as': 'supplier'
        }
    },
    {
        '$unwind': '$customer'
    },
    {
        '$unwind': '$supplier'
    },
    {
        '$match': {
            '$or': [
                {'$and': [
                    {'supplier.S_NATIONKEY': {'$in': list(nation_dict.keys())}},
                    {'customer.C_NATIONKEY': {'$in': list(nation_dict.keys())}},
                    {'supplier.S_NATIONKEY': {'$ne': '$customer.C_NATIONKEY'}}
                ]}
            ]
        }
    },
    {
        '$project': {
            'C_NATIONKEY': '$customer.C_NATIONKEY',
            'S_NATIONKEY': '$supplier.S_NATIONKEY',
            'L_YEAR': {'$year': '$L_SHIPDATE'},
            'REVENUE': {
                '$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]
            }
        }
    },
    {
        '$group': {
            '_id': {
                'C_NATIONKEY': '$C_NATIONKEY',
                'S_NATIONKEY': '$S_NATIONKEY',
                'L_YEAR': '$L_YEAR'
            },
            'REVENUE': {'$sum': '$REVENUE'}
        }
    },
    {
        '$sort': {
            '_id.S_NATIONKEY': 1,
            '_id.C_NATIONKEY': 1,
            '_id.L_YEAR': 1
        }
    },
    {
        '$project': {
            'C_NATIONKEY': '$_id.C_NATIONKEY',
            'S_NATIONKEY': '$_id.S_NATIONKEY',
            'REVENUE': '$REVENUE',
            'L_YEAR': '$_id.L_YEAR',
            '_id': 0
        }
    }
]

# Execute the MongoDB query
results = list(lineitem_collection.aggregate(pipeline))  # Convert the cursor to a list

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()

    # Convert nation keys to names and write each row
    for row in results:
        writer.writerow({
            'CUST_NATION': nation_dict[row['C_NATIONKEY']],
            'L_YEAR': row['L_YEAR'],
            'REVENUE': row['REVENUE'],
            'SUPP_NATION': nation_dict[row['S_NATIONKEY']]
        })

# Close the connections
mysql_cur.close()
mysql_conn.close()
mongo_client.close()
