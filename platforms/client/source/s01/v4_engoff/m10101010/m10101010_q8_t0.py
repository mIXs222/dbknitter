import csv
import pymysql
import pymongo
from datetime import datetime

# MySQL connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_tpch_db = mongo_client['tpch']

# Constants for data filtering
start_date_1995 = datetime(1995, 1, 1)
end_date_1995 = datetime(1995, 12, 31)
start_date_1996 = datetime(1996, 1, 1)
end_date_1996 = datetime(1996, 12, 31)

# Helper function for MongoDB aggregation
def aggregate_market_share(mongo_collection, start_date, end_date, type_match):
    pipeline = [
        {'$match': {'O_ORDERDATE': {'$gte': start_date, '$lte': end_date}}},
        {'$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }},
        {'$unwind': '$lineitems'},
        {'$match': {'lineitems.L_COMMENT': {'$regex': type_match}}},
        {'$project': {
            'revenue': {
                '$multiply': [
                    '$lineitems.L_EXTENDEDPRICE',
                    {'$subtract': [1, '$lineitems.L_DISCOUNT']}
                ]
            }
        }},
        {'$group': {'_id': None, 'total_revenue': {'$sum': '$revenue'}}}
    ]
    result = list(mongo_collection.aggregate(pipeline))
    return result[0]['total_revenue'] if result else 0

try:
    # MySQL query execution
    with mysql_connection.cursor() as mysql_cursor:
        mysql_cursor.execute(
            "SELECT sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue "
            "FROM lineitem, supplier, nation, region "
            "WHERE L_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY "
            "AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'ASIA' "
            "AND S_COMMENT LIKE '%INDIA%' AND L_COMMENT LIKE '%SMALL PLATED COPPER%'"
        )
        mysql_results = mysql_cursor.fetchall()

    # MongoDB query execution
    mongo_orders_collection = mongo_tpch_db['orders']
    mongo_revenue_1995 = aggregate_market_share(mongo_orders_collection, start_date_1995, end_date_1995, 'SMALL PLATED COPPER')
    mongo_revenue_1996 = aggregate_market_share(mongo_orders_collection, start_date_1996, end_date_1996, 'SMALL PLATED COPPER')

    # Combine results
    total_revenue_1995 = mysql_results[0][0] + mongo_revenue_1995
    total_revenue_1996 = mysql_results[0][0] + mongo_revenue_1996

    # Write the output to CSV
    with open('query_output.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Year', 'Market Share'])
        csv_writer.writerow(['1995', total_revenue_1995])
        csv_writer.writerow(['1996', total_revenue_1996])

finally:
    mysql_connection.close()
    mongo_client.close()
