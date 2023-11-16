# Import required libraries
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

# Retrieve relevant parts from MySQL
part_query = """
SELECT P_PARTKEY, P_BRAND, P_SIZE, P_CONTAINER
FROM part
WHERE
    (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5) OR
    (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10) OR
    (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
"""
mysql_cursor.execute(part_query)
parts = mysql_cursor.fetchall()

# Mapping of part keys to their respective attributes from the MySQL part table
part_attr_map = {(p[0], p[1], p[2], p[3]) for p in parts}
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['lineitem']

# Filtering lineitem entries in MongoDB according to the criteria defined by the attributes in the MySQL part table
pipeline = [
    {
        '$match': {
            '$or': [
                {'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 'L_SHIPINSTRUCT': 'DELIVER IN PERSON', 'L_QUANTITY': {'$gte': 1, '$lte': 11}},
                {'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 'L_SHIPINSTRUCT': 'DELIVER IN PERSON', 'L_QUANTITY': {'$gte': 10, '$lte': 20}},
                {'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}, 'L_SHIPINSTRUCT': 'DELIVER IN PERSON', 'L_QUANTITY': {'$gte': 20, '$lte': 30}}
            ]
        }
    },
    {
        '$project': {
            'L_PARTKEY': 1,
            'REVENUE': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}
        }
    }
]

lineitems = list(mongo_collection.aggregate(pipeline))

# Calculate total revenue by combining parts and lineitems data
total_revenue = 0
for lineitem in lineitems:
    if (lineitem['L_PARTKEY'],) in part_attr_map:
        total_revenue += lineitem['REVENUE']

# Write query output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['REVENUE'])
    writer.writerow([total_revenue])
