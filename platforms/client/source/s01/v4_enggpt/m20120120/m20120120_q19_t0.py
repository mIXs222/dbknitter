# query.py

import pymysql
import pymongo
import csv

# Connect to MySQL
connection = pymysql.connect(host='mysql', 
                             user='root',
                             password='my-secret-pw', 
                             database='tpch')
cursor = connection.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db.part

# Define the selection sets as per the given conditions
selection_sets = [
    {
        "brand": "Brand#12",
        "containers": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"],
        "quantity_low": 1,
        "quantity_high": 11,
        "size_low": 1,
        "size_high": 5
    },
    {
        "brand": "Brand#23",
        "containers": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"],
        "quantity_low": 10,
        "quantity_high": 20,
        "size_low": 1,
        "size_high": 10
    },
    {
        "brand": "Brand#34",
        "containers": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"],
        "quantity_low": 20,
        "quantity_high": 30,
        "size_low": 1,
        "size_high": 15
    }
]

# Generate a list of part keys that fulfill the MongoDB criteria
part_keys = set()
for selection in selection_sets:
    mongo_query = {
        "P_BRAND": selection["brand"],
        "P_CONTAINER": {"$in": selection["containers"]}
    }
    for part in part_collection.find(mongo_query):
        part_keys.add(part["P_PARTKEY"])

# Format the part keys for the SQL query
formatted_part_keys = ', '.join(str(key) for key in part_keys)

# Create the SQL selection conditions
sql_condition = ' OR '.join([
    f'''(
        L_PARTKEY IN ({formatted_part_keys}) AND
        L_QUANTITY BETWEEN {selection["quantity_low"]} AND {selection["quantity_high"]} AND
        P_SIZE BETWEEN {selection["size_low"]} AND {selection["size_high"]} AND
        L_SHIPMODE IN ('AIR', 'AIR REG') AND
        L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )'''
    for selection in selection_sets
])

# Construct the SQL query
sql_query = f"""
    SELECT
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
    FROM
        lineitem, part
    WHERE
        L_PARTKEY = P_PARTKEY AND ({sql_condition})
"""

# Run the SQL query
cursor.execute(sql_query)
result = cursor.fetchone()

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['revenue'])
    writer.writerow(result)

# Close the connections
cursor.close()
connection.close()
mongo_client.close()

