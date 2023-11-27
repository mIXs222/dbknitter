import pymysql
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB database
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']

# Define the query for MySQL (supplier table)
supplier_query = "SELECT S_SUPPKEY FROM supplier WHERE S_COMMENT NOT LIKE '%Customer%Complaints%'"

# Execute the query on MySQL
mysql_cursor.execute(supplier_query)
supplier_results = set(s['S_SUPPKEY'] for s in mysql_cursor.fetchall())

# Define the filters for MongoDB queries
part_filter = {
    'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]},
    'P_TYPE': {'$ne': 'MEDIUM POLISHED'},
    'P_BRAND': {'$ne': 'Brand#45'}
}

partsupp_pipeline = [
    {'$match': {'PS_SUPPKEY': {'$in': list(supplier_results)}}},
    {'$group': {'_id': {'PS_PARTKEY': '$PS_PARTKEY'}, 'suppliers_count': {'$sum': 1}}},
    {'$lookup': {
        'from': 'part',
        'localField': '_id.PS_PARTKEY',
        'foreignField': 'P_PARTKEY',
        'as': 'part'
    }},
    {'$unwind': '$part'},
    {'$match': {'part': part_filter}},
    {'$project': {
        'P_BRAND': '$part.P_BRAND',
        'P_TYPE': '$part.P_TYPE',
        'P_SIZE': '$part.P_SIZE',
        'suppliers_count': 1
    }},
    {'$sort': {'suppliers_count': -1, 'P_BRAND': 1, 'P_TYPE': 1, 'P_SIZE': 1}}
]

# Execute the aggregated query on MongoDB
partsupp_results = list(mongodb_db.partsupp.aggregate(partsupp_pipeline))

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'suppliers_count']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for record in partsupp_results:
        writer.writerow({
            'P_BRAND': record['P_BRAND'],
            'P_TYPE': record['P_TYPE'],
            'P_SIZE': record['P_SIZE'],
            'suppliers_count': record['suppliers_count'],
        })

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
