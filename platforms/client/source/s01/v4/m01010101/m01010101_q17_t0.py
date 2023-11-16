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
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_lineitem = mongo_db['lineitem']

# Execute SQL query on MySQL to get data from 'part' table
part_query = """
    SELECT P_PARTKEY, P_BRAND, P_CONTAINER
    FROM part
    WHERE
        P_BRAND = 'Brand#23'
        AND P_CONTAINER = 'MED BAG'
"""
mysql_cursor.execute(part_query)
part_result = mysql_cursor.fetchall()

# Set of P_PARTKEYs that satisfy the part query conditions
partkey_set = {row[0] for row in part_result}

# MongoDB aggregation to calculate the average quantity for each part
pipeline = [
    {"$match": {"L_PARTKEY": {"$in": list(partkey_set)}}},
    {
        "$group": {
            "_id": "$L_PARTKEY",
            "average_quantity": {"$avg": "$L_QUANTITY"}
        }
    }
]
avg_quantities = {doc['_id']: doc['average_quantity'] for doc in mongo_lineitem.aggregate(pipeline)}

# Query lineitem collection
l_extendedprice_sum = 0
for partkey in partkey_set:
    subquery_result = mongo_lineitem.find(
        {
            "L_PARTKEY": partkey,
            "L_QUANTITY": {"$lt": 0.2 * avg_quantities[partkey]}
        },
        {"L_EXTENDEDPRICE": 1}
    )

    for doc in subquery_result:
        l_extendedprice_sum += doc["L_EXTENDEDPRICE"]

avg_yearly = l_extendedprice_sum / 7.0

# Write result to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['AVG_YEARLY'])
    writer.writerow([avg_yearly])

# Closing the cursor and connection for MySQL
mysql_cursor.close()
mysql_conn.close()
