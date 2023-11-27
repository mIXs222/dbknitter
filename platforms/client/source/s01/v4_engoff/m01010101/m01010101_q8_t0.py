import pymysql
import pymongo
import csv

# Connection details
mysql_connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

mongodb_connection_info = {
    'host': 'mongodb',
    'port': 27017,
    'db': 'tpch',
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_connection_info)
mysql_cursor = mysql_conn.cursor()

# Get Indian suppliers' details from MySQL
mysql_cursor.execute("""
    SELECT s.S_SUPPKEY
    FROM supplier s
    JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
    JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
    WHERE n.N_NAME = 'INDIA' AND r.R_NAME = 'ASIA';
""")
indian_supplier_keys = [row[0] for row in mysql_cursor.fetchall()]

# Connect to MongoDB
mongo_client = pymongo.MongoClient(mongodb_connection_info['host'], mongodb_connection_info['port'])
mongodb = mongo_client[mongodb_connection_info['db']]

# Calculate market share for 1995 and 1996
market_share = {}
for year in [1995, 1996]:
    pipeline = [
        {'$match': {
            'L_SHIPDATE': {'$gte': f'{year}-01-01', '$lt': f'{year + 1}-01-01'},
            'L_PARTKEY': {'$in': indian_supplier_keys}
        }},
        {'$project': {
            'revenue': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}
        }},
        {'$group': {
            '_id': None,
            'total_revenue': {'$sum': '$revenue'}
        }}
    ]
    results = list(mongodb.lineitem.aggregate(pipeline))
    market_share[year] = results[0]['total_revenue'] if results else 0

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['YEAR', 'MARKET_SHARE'])
    for year, revenue in market_share.items():
        csvwriter.writerow([year, revenue])

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
