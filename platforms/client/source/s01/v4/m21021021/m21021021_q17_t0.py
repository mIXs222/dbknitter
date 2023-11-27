import pymongo
import pymysql
import csv

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_lineitem = mongo_db['lineitem']

# MySQL connection
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch',
                                   cursorclass=pymysql.cursors.Cursor)
mysql_cursor = mysql_connection.cursor()

# Query for MySQL to get parts
part_query = """
SELECT P_PARTKEY
FROM part
WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'
"""
mysql_cursor.execute(part_query)
parts_found = mysql_cursor.fetchall()
parts_keys = [part[0] for part in parts_found]

# Query MongoDB to get average quantity for the parts
pipeline = [
    {"$match": {"L_PARTKEY": {"$in": parts_keys}}},
    {"$group": {"_id": "$L_PARTKEY", "average_quantity": {"$avg": "$L_QUANTITY"}}}
]
lineitem_avg = mongo_lineitem.aggregate(pipeline)
avg_quantity_dict = {doc['_id']: doc['average_quantity'] for doc in lineitem_avg}

# Query MongoDB to calculate AVG_YEARLY
pipeline = [
    {
        "$match": {
            "L_PARTKEY": {"$in": parts_keys},
            "L_QUANTITY": {"$lt": 0}
        }
    },
    {
        "$group": {
            "_id": None,
            "AVG_YEARLY": {"$sum": "$L_EXTENDEDPRICE"}
        }
    }
]

# Update the $lt in the $match stage using the previously fetched average quantities
for part_key, avg_quantity in avg_quantity_dict.items():
    pipeline[0]['$match']['L_QUANTITY']['$lt'] = 0.2 * avg_quantity

results = mongo_lineitem.aggregate(pipeline)
avg_yearly = next(results, {}).get('AVG_YEARLY', 0)

if avg_yearly is not None:
    avg_yearly /= 7.0  # Calculate the actual AVG_YEARLY value

# Write output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['AVG_YEARLY'])
    writer.writerow([avg_yearly])

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
