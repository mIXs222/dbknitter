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
mysql_cursor = mysql_conn.cursor()

# Fetch part data from MySQL database
mysql_cursor.execute("""
SELECT P_PARTKEY, P_BRAND, P_CONTAINER, P_SIZE
FROM part
WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
""")
parts = {row[0]:row for row in mysql_cursor.fetchall()}

mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']

# Set the query for lineitems in MongoDB
lineitem_query = {
    "$or": [
        {
            "L_QUANTITY": {"$gte": 1, "$lte": 11},
            "L_SHIPMODE": {"$in": ['AIR', 'AIR REG']},
            "L_SHIPINSTRUCT": "DELIVER IN PERSON"
        },
        {
            "L_QUANTITY": {"$gte": 10, "$lte": 20},
            "L_SHIPMODE": {"$in": ['AIR', 'AIR REG']},
            "L_SHIPINSTRUCT": "DELIVER IN PERSON"
        },
        {
            "L_QUANTITY": {"$gte": 20, "$lte": 30},
            "L_SHIPMODE": {"$in": ['AIR', 'AIR REG']},
            "L_SHIPINSTRUCT": "DELIVER IN PERSON"
        }
    ]
}

# Fetch lineitem data from MongoDB
lineitems = list(lineitem_collection.find(lineitem_query))

# Initialize revenue
revenue = 0.0

# Calculate revenue from the join of part and lineitem on P_PARTKEY = L_PARTKEY
for lineitem in lineitems:
    part = parts.get(lineitem['L_PARTKEY'])
    if part:
        revenue += lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['REVENUE'])
    csvwriter.writerow([revenue])
