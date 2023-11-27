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
mongo_collection = mongo_db['lineitem']

# Define the conditions for each type
types = [
    {"brand_id": "12", "containers": ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'], "quantity_range": (1, 11), "size_range": (1, 5)},
    {"brand_id": "23", "containers": ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'], "quantity_range": (10, 20), "size_range": (1, 10)},
    {"brand_id": "34", "containers": ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'], "quantity_range": (20, 30), "size_range": (1, 15)}
]

# Query MySQL for the part keys of the specified types
part_keys = []
for typ in types:
    query = f"""
        SELECT P_PARTKEY FROM part
        WHERE
            P_BRAND = 'Brand#{typ["brand_id"]}' AND
            P_CONTAINER IN {tuple(typ["containers"])} AND
            P_SIZE BETWEEN {typ["size_range"][0]} AND {typ["size_range"][1]};
    """
    mysql_cursor.execute(query)
    part_keys.extend([row[0] for row in mysql_cursor.fetchall()])

# Query MongoDB for line items that match the part keys and conditions
results = []
for typ in types:
    query = {
        'L_PARTKEY': {'$in': part_keys},
        'L_QUANTITY': {'$gte': typ['quantity_range'][0], '$lte': typ["quantity_range"][1]},
        'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
        'L_SHIPINSTRUCT': 'DELIVER IN PERSON'
    }
    cursor = mongo_collection.find(query)
    for document in cursor:
        results.append({
            'L_ORDERKEY': document['L_ORDERKEY'],
            'L_EXTENDEDPRICE': document['L_EXTENDEDPRICE'],
            'L_DISCOUNT': document['L_DISCOUNT'],
            'L_QUANTITY': document['L_QUANTITY']
        })

# Calculate discounted revenue and write to csv
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_ORDERKEY', 'REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for lineitem in results:
        revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
        writer.writerow({'L_ORDERKEY': lineitem['L_ORDERKEY'], 'REVENUE': revenue})

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
