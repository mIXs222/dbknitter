import csv
import pymysql
from pymongo import MongoClient

# Function to execute query on MySQL
def query_mysql(connection_params, exclude_suppkeys):
    conn = pymysql.connect(**connection_params)
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT
                    S_SUPPKEY
                FROM
                    supplier
                WHERE
                    S_COMMENT LIKE '%Customer%Complaints%'
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            # Exclude supplier keys found in supplier table with comments
            # matching '%Customer%Complaints%'
            for row in rows:
                exclude_suppkeys.add(row[0])
    finally:
        conn.close()
    return exclude_suppkeys

# Function to execute query on MongoDB
def query_mongodb(connection_params, exclude_suppkeys):
    client = MongoClient(**connection_params)
    db = client[connection_params['db']]
    part_collection = db['part']
    partsupp_collection = db['partsupp']

    pipeline = [
        {
            '$match': {
                'P_BRAND': {'$ne': 'Brand#45'},
                'P_TYPE': {'$not': {'$regex': '^MEDIUM POLISHED'}},
                'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]}
            }
        },
        {
            '$lookup': {
                'from': 'partsupp',
                'localField': 'P_PARTKEY',
                'foreignField': 'PS_PARTKEY',
                'as': 'supply_info'
            }
        },
        {'$unwind': '$supply_info'},
        {
            '$match': {
                'supply_info.PS_SUPPKEY': {'$nin': list(exclude_suppkeys)}
            }
        },
        {
            '$group': {
                '_id': {
                    'P_BRAND': '$P_BRAND',
                    'P_TYPE': '$P_TYPE',
                    'P_SIZE': '$P_SIZE',
                },
                'SUPPLIER_CNT': {'$addToSet': '$supply_info.PS_SUPPKEY'}
            }
        },
        {
            '$project': {
                'P_BRAND': '$_id.P_BRAND',
                'P_TYPE': '$_id.P_TYPE',
                'P_SIZE': '$_id.P_SIZE',
                'SUPPLIER_CNT': {'$size': '$SUPPLIER_CNT'}
            }
        },
        {'$sort': {'SUPPLIER_CNT': -1, 'P_BRAND': 1, 'P_TYPE': 1, 'P_SIZE': 1}}
    ]
    
    results = part_collection.aggregate(pipeline)
    client.close()
    
    return results

# MySQL connection parameters
mysql_connection_params = {
    'host':'mysql',
    'user':'root',
    'password':'my-secret-pw',
    'db':'tpch'
}

# MongoDB connection parameters
mongodb_connection_params = {
    'host':'mongodb',
    'port': 27017,
    'db':'tpch'
}

# Exclude supplier keys based on MySQL data
exclude_suppkeys = set()
exclude_suppkeys = query_mysql(mysql_connection_params, exclude_suppkeys)

# Retrieve the rest of the data from MongoDB and process query
results = query_mongodb(mongodb_connection_params, exclude_suppkeys)

# Write query output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow({
            'P_BRAND': result['P_BRAND'],
            'P_TYPE': result['P_TYPE'],
            'P_SIZE': result['P_SIZE'],
            'SUPPLIER_CNT': result['SUPPLIER_CNT']
        }) 
