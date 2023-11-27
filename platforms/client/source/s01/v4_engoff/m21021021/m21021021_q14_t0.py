# query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Constants
MYSQL_HOST = 'mysql'
MYSQL_DB = 'tpch'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'my-secret-pw'

MONGO_HOST = 'mongodb'
MONGO_PORT = 27017
MONGO_DB = 'tpch'

# Date range for the query
DATE_FORMAT = "%Y-%m-%d"
START_DATE = datetime.strptime("1995-09-01", DATE_FORMAT)
END_DATE = datetime.strptime("1995-10-01", DATE_FORMAT)

# Connect to MySQL
mysql_connection = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
mysql_cursor = mysql_connection.cursor()

# Query to select all parts that are considered promotional
mysql_cursor.execute("SELECT P_PARTKEY FROM part WHERE P_TYPE LIKE 'PROMO%'")
promotional_parts = set(row[0] for row in mysql_cursor.fetchall())

# Connect to MongoDB
mongo_client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
mongo_db = mongo_client[MONGO_DB]
lineitem_collection = mongo_db.lineitem

# Query to calculate revenue for shipped parts
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': START_DATE, '$lt': END_DATE},
            'L_PARTKEY': {'$in': list(promotional_parts)}
        }
    },
    {
        '$project': {
            'revenue': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}
        }
    },
    {
        '$group': {
            '_id': None,
            'total_revenue': {'$sum': '$revenue'}
        }
    }
]

result = list(lineitem_collection.aggregate(pipeline))
total_revenue = result[0]['total_revenue'] if result else 0

# Writing the output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['total_revenue'])
    writer.writerow([total_revenue])

# Close connections
mysql_cursor.close()
mysql_connection.close()

