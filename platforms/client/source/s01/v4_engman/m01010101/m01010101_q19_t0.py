# execute_query.py
import pymysql
import pymongo
import csv

# Function to calculate revenue for a given brand_id, container types, quantity and size
def calculate_revenue(brand_id, containers, quantity_range, size_range):
    mysql_conn = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')
    mongo_conn = pymongo.MongoClient('mongodb', 27017)
    mongo_db = mongo_conn['tpch']

    # Construct the part selection query for MySQL
    part_selection = "P_BRAND = '{}' AND P_CONTAINER IN ({}) AND P_SIZE BETWEEN {} AND {}"
    formatted_containers = ', '.join("'{}'".format(c) for c in containers)
    part_selection = part_selection.format(brand_id, formatted_containers, size_range[0], size_range[1])

    # MySQL query for fetching part keys
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(f"SELECT P_PARTKEY FROM part WHERE {part_selection}")

    part_keys = [row[0] for row in mysql_cursor.fetchall()]
    mysql_conn.close()

    # MongoDB query for summing up the revenue using the part keys from MySQL query
    pipeline = [
        {"$match": {
            "L_PARTKEY": {"$in": part_keys},
            "L_QUANTITY": {"$gte": quantity_range[0], "$lte": quantity_range[1]},
            "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]},
            "L_SHIPINSTRUCT": "DELIVER IN PERSON"
        }},
        {"$group": {
            "_id": None,
            "REVENUE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}}
        }}
    ]
    result = list(mongo_db.lineitem.aggregate(pipeline))

    revenue = result[0]['REVENUE'] if result else 0
    mongo_conn.close()

    return revenue


# Define part properties
part_properties = [
    {'brand_id': '12', 'containers': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'], 'quantity_range': (1, 11), 'size_range': (1, 5)},
    {'brand_id': '23', 'containers': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'], 'quantity_range': (10, 20), 'size_range': (1, 10)},
    {'brand_id': '34', 'containers': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'], 'quantity_range': (20, 30), 'size_range': (1, 15)}
]

# Calculate revenue for each part type and sum them
total_revenue = sum(calculate_revenue(**props) for props in part_properties)

# Write the result to query_output.csv
with open('query_output.csv', 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['REVENUE'])
    writer.writerow([total_revenue])
