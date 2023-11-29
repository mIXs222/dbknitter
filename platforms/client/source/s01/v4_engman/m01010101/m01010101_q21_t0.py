import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Execute the query on MySQL to get nation keys for 'SAUDI ARABIA'
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT N_NATIONKEY
        FROM nation
        WHERE N_NAME = 'SAUDI ARABIA';
    """)
    saudi_nation_key = cursor.fetchone()[0]

mysql_conn.close()

# Query MongoDB for supplier information
suppliers = mongo_db['supplier'].find({'S_NATIONKEY': saudi_nation_key})

# Map supplier key to supplier name where orders have status 'F'
supplier_name_map = {}
for supplier in suppliers:
    supplier_name_map[supplier['S_SUPPKEY']] = supplier['S_NAME']

# Query MongoDB for lineitems with status 'F' and only one supplier failing to meet the date
lineitems = mongo_db['lineitem'].aggregate([
    {
        '$match': {
            'L_RETURNFLAG': 'F',
            'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'}
        }
    },
    {'$group': {
        '_id': '$L_ORDERKEY',
        'suppliers': {'$push': '$L_SUPPKEY'}
    }},
    {'$match': {
        'suppliers': {'$size': 1}
    }}
])

# Count the awaited lineitems for each supplier
numwait_map = {}
for lineitem in lineitems:
    suppliers = lineitem['suppliers']
    if suppliers[0] in numwait_map:
        numwait_map[suppliers[0]] += 1
    else:
        numwait_map[suppliers[0]] = 1

# Prepare the final output
output_data = [{'NUMWAIT': numwait_map[s], 'S_NAME': supplier_name_map[s]} for s in numwait_map]
output_data = sorted(output_data, key=lambda x: (-x['NUMWAIT'], x['S_NAME']))

# Write to CSV
with open('query_output.csv', mode='w') as csv_file:
    fieldnames = ['NUMWAIT', 'S_NAME']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for row in output_data:
        writer.writerow(row)
