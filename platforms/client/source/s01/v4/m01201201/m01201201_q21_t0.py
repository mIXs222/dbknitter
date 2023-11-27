import pymysql
import pymongo
import csv

# MySQL Database Connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB Database Connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

# Execute MySQL Query
mysql_cursor = mysql_connection.cursor()
mysql_cursor.execute("""
SELECT
    S_NAME,
    S_SUPPKEY,
    S_NATIONKEY
FROM
    supplier,
    nation
WHERE
    S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'SAUDI ARABIA'
""")
suppliers = mysql_cursor.fetchall()

# Execute MongoDB Aggregate Query
pipeline = [
    {
        '$match': {
            'L_ORDERSTATUS': 'F',
            'L_RECEIPTDATE': {'$gt': '$L_COMMITDATE'}
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'NUMWAIT': {'$sum': 1}
        }
    }
]
lineitems = list(lineitem_collection.aggregate(pipeline))

# Join Results & Filter according to SQL Query Conditions
results = {}
for supplier in suppliers:
    s_name, s_suppkey, s_nationkey = supplier
    # Get NUMWAIT from lineitems
    numwait = next((item['NUMWAIT'] for item in lineitems if item['_id'] == s_suppkey), 0)

    # Avoid suppliers without NUMWAIT
    if numwait > 0:
        results[s_suppkey] = (s_name, numwait)

# Sort Results
sorted_results = sorted(results.values(), key=lambda x: (-x[1], x[0]))

# Write Results to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_NAME', 'NUMWAIT'])
    for row in sorted_results:
        writer.writerow(row)

# Close Connections
mysql_cursor.close()
mysql_connection.close()
mongodb_client.close()
