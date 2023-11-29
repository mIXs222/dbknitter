# query.py
import pymysql.cursors
import pymongo
import csv

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Connect to MongoDB database
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Query MySQL to fetch customer information
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("SELECT C_CUSTKEY FROM customer")
customer_ids = [row[0] for row in mysql_cursor.fetchall()]
mysql_cursor.close()

# Query MongoDB to fetch order information
orders_collection = mongo_db['orders']
order_count_by_customer = orders_collection.aggregate([
    {
        '$match': {
            'O_ORDERSTATUS': {'$nin': ['pending', 'deposits']},
            'O_COMMENT': {'$not': {'$regex': '.*pending.*deposits.*'}}
        }
    },
    {
        '$group': {
            '_id': '$O_CUSTKEY',
            'order_count': {'$sum': 1}
        }
    },
    {
        '$match': {
            '_id': {'$in': customer_ids}
        }
    },
    {
        '$group': {
            '_id': '$order_count',
            'customer_count': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id': 1}
    }
])

# Write query results to file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['OrderCount', 'CustomerCount'])
    for doc in order_count_by_customer:
        writer.writerow([doc['_id'], doc['customer_count']])

# Close connections
mysql_conn.close()
mongo_client.close()
