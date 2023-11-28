# query.py
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

# Connect to MongoDB
mongo_client = pymongo.MongoClient(
    host='mongodb',
    port=27017
)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# MongoDB query to filter parts
part_conditions = [
    {"$match": {
        "P_BRAND": "Brand#12",
        "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}
    }},
    {"$match": {
        "P_BRAND": "Brand#23",
        "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}
    }},
    {"$match": {
        "P_BRAND": "Brand#34",
        "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}
    }}
]
part_keys_by_brand = {brand: [] for brand in ["Brand#12", "Brand#23", "Brand#34"]}

# Query each condition and store the part keys
for condition in part_conditions:
    parts_cursor = part_collection.find(condition)
    for part in parts_cursor:
        part_keys_by_brand[part["P_BRAND"]].append(part["P_PARTKEY"])

# MySQL query to select the relevant line items
lineitem_select_statement = """
SELECT
    L_EXTENDEDPRICE, L_DISCOUNT
FROM
    lineitem
WHERE
    L_PARTKEY IN %s AND L_QUANTITY >= %s AND L_QUANTITY <= %s
    AND L_SIZE >= %s AND L_SIZE <= %s
    AND L_SHIPMODE IN %s AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
"""

# Parameters for MySQL query
conditions = [
    (part_keys_by_brand["Brand#12"], 1, 11, 1, 5, ['AIR', 'AIR REG']),
    (part_keys_by_brand["Brand#23"], 10, 20, 1, 10, ['AIR', 'AIR REG']),
    (part_keys_by_brand["Brand#34"], 20, 30, 1, 15, ['AIR', 'AIR REG'])
]

# Calculate revenue and write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Revenue'])  # Writing header row

    with mysql_conn.cursor() as cursor:
        total_revenue = 0.0
        for part_keys, qty_min, qty_max, size_min, size_max, ship_modes in conditions:
            if part_keys:  # If there are part keys to query
                cursor.execute(lineitem_select_statement, (part_keys, qty_min, qty_max, size_min, size_max, ship_modes))
                lineitems = cursor.fetchall()
                for price, discount in lineitems:
                    revenue = price * (1 - discount)
                    total_revenue += revenue
                    writer.writerow([revenue])

# close the connections
mysql_conn.close()
mongo_client.close()
