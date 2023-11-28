# query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# MySQL query
mysql_query = """
    SELECT 
        n.N_NAME, 
        SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue
    FROM 
        orders o 
    INNER JOIN 
        lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
    INNER JOIN 
        customer c ON o.O_CUSTKEY = c.C_CUSTKEY
    INNER JOIN 
        nation n ON c.C_NATIONKEY = n.N_NATIONKEY
    INNER JOIN 
        region r ON n.N_REGIONKEY = r.R_REGIONKEY
    WHERE 
        r.R_NAME = 'ASIA'
        AND o.O_ORDERDATE >= '1990-01-01'
        AND o.O_ORDERDATE <= '1994-12-31'
    GROUP BY 
        n.N_NAME
    ORDER BY 
        revenue DESC;
"""

# MongoDB aggregation
mongo_aggregation = [
    {'$match': {'R_NAME': 'ASIA'}},
    {'$lookup': {
        'from': 'nation',
        'localField': 'R_REGIONKEY',
        'foreignField': 'N_REGIONKEY',
        'as': 'nation'
    }},
    {'$unwind': '$nation'},
    {'$lookup': {
        'from': 'customer',
        'localField': 'nation.N_NATIONKEY',
        'foreignField': 'C_NATIONKEY',
        'as': 'customer'
    }},
    {'$unwind': '$customer'},
    {'$lookup': {
        'from': 'supplier',
        'localField': 'nation.N_NATIONKEY',
        'foreignField': 'S_NATIONKEY',
        'as': 'supplier'
    }},
    {'$unwind': '$supplier'},
    {'$lookup': {
        'from': 'lineitem',
        'let': {'supplier_key': '$supplier.S_SUPPKEY', 'order_date': '$customer.C_CUSTKEY'},
        'pipeline': [{
            '$match': {
                '$expr': {
                    '$and': [
                        {'$eq': ['$L_SUPPKEY', '$$supplier_key']},
                        {
                            '$gte': [{'$dateFromString': {'dateString': '$L_SHIPDATE'}}, datetime(1990, 1, 1)]
                        },
                        {
                            '$lte': [{'$dateFromString': {'dateString': '$L_SHIPDATE'}}, datetime(1994, 12, 31)]
                        }
                    ]
                }
            }
        }],
        'as': 'lineitem'
    }},
    {'$unwind': '$lineitem'},
    {'$project': {
        'nation_name': '$nation.N_NAME',
        'lineitem_price': {
            '$multiply': [
                '$lineitem.L_EXTENDEDPRICE',
                {'$subtract': [1, '$lineitem.L_DISCOUNT']}
            ]
        }
    }},
    {'$group': {
        '_id': '$nation_name',
        'revenue': {'$sum': '$lineitem_price'}
    }},
    {'$sort': {'revenue': -1}}
]

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_data = cursor.fetchall()

# Execute MongoDB query
mongo_data = mongodb['region'].aggregate(mongo_aggregation)

# Process MongoDB data
mongo_results = []
for data in mongo_data:
    mongo_results.append((data['_id'], data['revenue']))

# Combine data from MySQL and Mongo results (assuming duplicates do not exist between databases)
combined_results = mysql_data + mongo_results

# Write result to csv file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['N_NAME', 'REVENUE'])
    csvwriter.writerows(combined_results)

# Close connections
mysql_conn.close()
mongo_client.close()
