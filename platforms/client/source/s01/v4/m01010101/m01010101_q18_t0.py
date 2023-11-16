import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
customer_collection = mongo_db["customer"]
lineitem_collection = mongo_db["lineitem"]

# Execute the lineitem subquery in MongoDB
pipeline = [
    {
        "$group": {
            "_id": "$L_ORDERKEY",
            "total_quantity": {"$sum": "$L_QUANTITY"}
        }
    },
    {
        "$match": {
            "total_quantity": {"$gt": 300}
        }
    }
]
lineitem_subquery_results = list(lineitem_collection.aggregate(pipeline))
orderkeys_with_large_quantity = [doc['_id'] for doc in lineitem_subquery_results]

# Split list to chunks for MySQL IN clause
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Query MySQL Database
with mysql_conn.cursor() as cursor, open('query_output.csv', 'w', newline='') as csvfile:
    filewriter = csv.writer(csvfile)
    
    # Write headers
    filewriter.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM(L_QUANTITY)'])

    # Fetch data in chunks to avoid exceeding max_allowed_packet for IN clause
    for chunk in chunks(orderkeys_with_large_quantity, 1000):
        placeholders = ','.join(['%s'] * len(chunk))
        mysql_query = f"""
            SELECT
                C_NAME,
                C_CUSTKEY,
                O_ORDERKEY,
                O_ORDERDATE,
                O_TOTALPRICE,
                (SELECT SUM(L_QUANTITY) FROM lineitem WHERE L_ORDERKEY IN ({placeholders}))
            FROM
                orders
            INNER JOIN
                customer ON C_CUSTKEY = O_CUSTKEY
            WHERE
                O_ORDERKEY IN ({placeholders})
            ORDER BY
                O_TOTALPRICE DESC,
                O_ORDERDATE
        """
        cursor.execute(mysql_query, tuple(chunk) * 2)
        results = cursor.fetchall()
        # Write data rows
        for row in results:
            filewriter.writerow(row)

# Close connections
mysql_conn.close()
mongo_client.close()
