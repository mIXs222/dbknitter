# Python code to execute the complex query across MySQL and MongoDB databases
import pymysql
import pymongo
import csv

# MySQL connection setup
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query setup and execution
with mysql_connection.cursor() as mysql_cursor:
    # Perform the SQL query to fetch relevant parts
    mysql_cursor.execute("""
        SELECT P_PARTKEY, P_BRAND, P_CONTAINER, P_SIZE, P_RETAILPRICE 
        FROM part 
        WHERE (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND P_SIZE BETWEEN 1 AND 5)
           OR (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND P_SIZE BETWEEN 1 AND 10)
           OR (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15)
    """)
    part_results = mysql_cursor.fetchall()

# Prepare the list of part keys from SQL query results for MongoDB query
part_keys_list = [row[0] for row in part_results]

# Fetch the lineitems from MongoDB matching the part keys
lineitem_collection = mongo_db['lineitem']
lineitem_cursor = lineitem_collection.find({
    'L_PARTKEY': {'$in': part_keys_list},
    'L_QUANTITY': {'$lte': 30},
    'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
    'L_SHIPINSTRUCT': 'DELIVER IN PERSON'
})

# Process the query results and write them to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['PartKey', 'Revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    
    # Aggregate and calculate the revenue for each part
    revenue_by_part = {}
    for lineitem in lineitem_cursor:
        if lineitem['L_PARTKEY'] in revenue_by_part:
            revenue_by_part[lineitem['L_PARTKEY']] += lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
        else:
            revenue_by_part[lineitem['L_PARTKEY']] = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    
    for part in part_results:
        part_key = part[0]
        if part_key in revenue_by_part:
            writer.writerow({'PartKey': part_key, 'Revenue': revenue_by_part[part_key]})
        else:
            writer.writerow({'PartKey': part_key, 'Revenue': 0})

# Close the connections
mysql_connection.close()
mongo_client.close()
