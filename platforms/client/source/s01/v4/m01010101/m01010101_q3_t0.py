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
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongodb_client['tpch']

# Query MySQL for orders with the specified O_ORDERDATE
sql_query = """
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_SHIPPRIORITY
FROM orders
WHERE O_ORDERDATE < '1995-03-15'
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(sql_query)
    orders_data = cursor.fetchall()

orders_filtered = {order[0]: order[1:] for order in orders_data}

# Query MongoDB for customers with C_MKTSEGMENT 'BUILDING'
customers = list(mongo_db.customer.find({"C_MKTSEGMENT": "BUILDING"}, {"_id": 0, "C_CUSTKEY": 1}))

custom_keys = [c['C_CUSTKEY'] for c in customers]

# Filter orders_data based on customer keys
orders_filtered = {k: v for k, v in orders_filtered.items() if v[0] in custom_keys}

# Query MongoDB for lineitems that match the filtered orders and have L_SHIPDATE > '1995-03-15'
lineitems = mongo_db.lineitem.aggregate([
    {"$match": {'L_ORDERKEY': {'$in': list(orders_filtered.keys())}, 'L_SHIPDATE': {'$gt': '1995-03-15'}}},
    {"$project": {
        "L_ORDERKEY": 1,
        "REVENUE": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}
    }}
])

# Merge the data and perform group by L_ORDERKEY, O_ORDERDATE, O_ORDERPRIORITY
aggregated = {}

for lineitem in lineitems:
    orderkey = lineitem['L_ORDERKEY']
    
    if orderkey in aggregated:
        aggregated[orderkey]['REVENUE'] += lineitem['REVENUE']
    else:
        aggregated[orderkey] = {
            'L_ORDERKEY': orderkey,
            'REVENUE': lineitem['REVENUE'],
            'O_ORDERDATE': orders_filtered[orderkey][0],
            'O_SHIPPRIORITY': orders_filtered[orderkey][1]
        }

# Sort the results
results = sorted(aggregated.values(), key=lambda x: (-x['REVENUE'], x['O_ORDERDATE']))

# Write the results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])

    for result in results:
        csv_writer.writerow([result['L_ORDERKEY'], result['REVENUE'], result['O_ORDERDATE'], result['O_SHIPPRIORITY']])

# Close connections
mysql_conn.close()
mongodb_client.close()
