import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Execute MySQL query to fetch nation data
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
nations = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# MongoDB pipeline to aggregate data
pipeline = [
    {
        '$match': {
            'O_ORDERDATE': {
                '$gte': datetime(1993, 10, 1),
                '$lte': datetime(1994, 1, 1)
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
        '$unwind': '$lineitems'
    },
    {
        '$match': {
            'lineitems.L_RETURNFLAG': 'R'
        }
    },
    {
        '$group': {
            '_id': '$O_CUSTKEY',
            'lost_revenue': {
                '$sum': {
                    '$multiply': [
                        "$lineitems.L_EXTENDEDPRICE",
                        {'$subtract': [1, "$lineitems.L_DISCOUNT"]}
                    ]
                }
            }
        }
    }
]

# Execute MongoDB query
customers_cursor = mongodb['orders'].aggregate(pipeline)

# Join with the customer table to fetch additional details
customers = list(mongodb['customer'].find())
customer_details = {c['C_CUSTKEY']: c for c in customers}

# Merge results
final_results = []
for res in customers_cursor:
    cust_key = res['_id']
    lost_revenue = res['lost_revenue']
    c_details = customer_details[cust_key]
    nation_name = nations.get(c_details['C_NATIONKEY'], '')
    final_results.append((
        c_details['C_NAME'],
        c_details['C_ADDRESS'],
        nation_name,
        c_details['C_PHONE'],
        c_details['C_ACCTBAL'],
        c_details['C_COMMENT'],
        lost_revenue,
        cust_key
    ))

# Sort the result
final_results.sort(key=lambda x: (-x[6], x[7], x[0], x[4]))

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT',
                     'LOST_REVENUE', 'C_CUSTKEY'])
    writer.writerows(final_results)

# Close MongoDB connection
mongo_client.close()
