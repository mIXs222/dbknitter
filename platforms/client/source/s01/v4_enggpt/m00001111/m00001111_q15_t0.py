import pymysql
import pymongo
from datetime import datetime
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Date range for the analysis
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)

# Query line items from MongoDB and calculate revenue
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': start_date,
                '$lte': end_date
            }
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'total_revenue': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE', 
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        }
    },
    {
        '$sort': {'total_revenue': -1}
    }
]
revenue_results = list(lineitem_collection.aggregate(pipeline))

# Find the supplier with the maximum revenue
max_revenue_sup = revenue_results[0]['_id'] if revenue_results else None

# Query the supplier details from MySQL
supplier_query = """
SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE 
FROM supplier 
WHERE S_SUPPKEY = %s
ORDER BY S_SUPPKEY ASC;
"""

# Execute the MySQL query
mysql_cursor.execute(supplier_query, (max_revenue_sup,))
supplier_result = mysql_cursor.fetchall()

# Prepare the CSV output
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    # Write the headers
    csv_writer.writerow([
        'S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'
    ])
    # Write the maximum revenue supplier data
    if max_revenue_sup and supplier_result:
        csv_writer.writerow([
            supplier_result[0][0], 
            supplier_result[0][1], 
            supplier_result[0][2], 
            supplier_result[0][3],
            revenue_results[0]['total_revenue']
        ])

# Close the connections
mongo_client.close()
mysql_cursor.close()
mysql_conn.close()
