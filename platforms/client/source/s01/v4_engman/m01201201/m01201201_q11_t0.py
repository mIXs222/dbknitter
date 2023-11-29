import pymysql
import pymongo
import csv

# Connect to mysql
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to mongodb
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_partsupp = mongo_db['partsupp']

# Execute the SQL query to get N_NATIONKEY
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY';")
    nationkey_row = cursor.fetchone()
    nation_key_germany = nationkey_row[0] if nationkey_row else None

    # Execute SQL query to get suppliers in GERMANY
    cursor.execute(
        "SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY = %s;", (nation_key_germany,))
    suppliers = cursor.fetchall()

supplier_keys = [str(supplier[0]) for supplier in suppliers]

# Query in mongodb to get the important stock
important_parts = []
if supplier_keys:
    pipeline = [
        {'$match': {'PS_SUPPKEY': {'$in': supplier_keys}}},
        {
            '$group': {
                '_id': None,
                'total_value': {'$sum': {'$multiply': ['$PS_AVAILQTY', '$PS_SUPPLYCOST']}}
            }
        },
        {
            '$project': {
                'part_value': {
                    '$multiply': ['$PS_AVAILQTY', '$PS_SUPPLYCOST']
                }
            }
        },
        {'$match': {'part_value': {'$gt': 0.0001}}},
        {'$sort': {'part_value': -1}}
    ]
    important_parts_cursor = mongo_partsupp.aggregate(pipeline)
    important_parts = list(important_parts_cursor)

# Combine results and write to csv
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['PS_PARTKEY', 'part_value']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for part in important_parts:
        writer.writerow({'PS_PARTKEY': part.get('PS_PARTKEY'), 'part_value': part.get('part_value')})

# Close connections
mysql_conn.close()
mongo_client.close()
