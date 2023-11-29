import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Find ASIA region key from MySQL
with mysql_conn.cursor() as mysql_cursor:
    mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_REGIONKEY = (SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA')")
    nations_in_asia = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Initial dictionary to hold revenue per nation
nation_revenues = {nation: 0 for nation in nations_in_asia.values()}

# Find the revenue volume from MongoDB
lineitem_aggregate = mongodb['lineitem'].aggregate([
    {
        '$match': {
            'L_SHIPDATE': {'$gte': '1990-01-01', '$lt': '1995-01-01'}
        }
    },
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'order'
        }
    },
    {
        '$unwind': '$order'
    },
    {
        '$lookup': {
            'from': 'customer',
            'localField': 'order.O_CUSTKEY',
            'foreignField': 'C_CUSTKEY',
            'as': 'customer'
        }
    },
    {
        '$unwind': '$customer'
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
        '$unwind': '$supplier'
    },
    {
        '$project': {
            'revenue': {
                '$multiply': [
                    '$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}
                ]
            },
            'nationkey_customer': '$customer.C_NATIONKEY',
            'nationkey_supplier': '$supplier.S_NATIONKEY'
        }
    },
    {
        '$match': {
            'nationkey_customer': {'$in': list(nations_in_asia.keys())},
            'nationkey_supplier': {'$in': list(nations_in_asia.keys())},
        }
    },
    {
        '$group': {
            '_id': '$nationkey_supplier',
            'revenue': {'$sum': '$revenue'}
        }
    }
])

# Accumulate revenue by nation
for doc in lineitem_aggregate:
    nation_revenues[nations_in_asia[doc['_id']]] += doc['revenue']

# Close connections
mysql_conn.close()
mongo_client.close()

# Sort the results by revenue and write to CSV
sorted_nations_by_revenue = sorted(nation_revenues.items(), key=lambda x: x[1], reverse=True)

with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['N_NAME', 'REVENUE'])
    for name, revenue in sorted_nations_by_revenue:
        writer.writerow([name, revenue])
