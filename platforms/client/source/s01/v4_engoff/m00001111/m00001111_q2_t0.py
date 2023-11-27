import pymysql
import pymongo
import csv

# Connection info for MySQL
mysql_connection_info = {
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql",
    "database": "tpch",
}

# Connect to MySQL
mysql_connection = pymysql.connect(**mysql_connection_info)
mysql_cursor = mysql_connection.cursor()

# First, get suppliers from the EUROPE region supplying BRASS parts of size 15
mysql_cursor.execute("""
SELECT
    s.S_ACCTBAL, s.S_NAME, n.N_NAME, p.P_PARTKEY, p.P_MFGR, s.S_ADDRESS, s.S_PHONE, s.S_COMMENT
FROM
    part p
JOIN
    supplier s ON s.S_NATIONKEY = n.N_NATIONKEY
JOIN
    nation n ON n.N_REGIONKEY = r.R_REGIONKEY
JOIN
    region r ON r.R_NAME = 'EUROPE'
WHERE
    p.P_TYPE = 'BRASS' AND p.P_SIZE = 15
ORDER BY
    s.S_ACCTBAL DESC, n.N_NAME, s.S_NAME, p.P_PARTKEY
""")

# Store the intermediate result
parts_suppliers = mysql_cursor.fetchall()

# Close the MySQL connection
mysql_cursor.close()
mysql_connection.close()

# Connection info for MongoDB
mongodb_connection_info = {
    "port": 27017,
    "host": "mongodb",
}

# Connect to MongoDB
mongo_client = pymongo.MongoClient(**mongodb_connection_info)
mongodb = mongo_client['tpch']
partsupp_collection = mongodb['partsupp']

# Find the minimum supply cost for each part.
min_supply_costs = partsupp_collection.aggregate([
    {
        "$group": {
            "_id": "$PS_PARTKEY",
            "minCost": {"$min": "$PS_SUPPLYCOST"}
        }
    }
])

# Convert the aggregation results to a dictionary for faster lookup
min_cost_dict = {doc["_id"]: doc["minCost"] for doc in min_supply_costs}

# Filter the MySQL results to keep only the suppliers with the minimum cost
min_cost_suppliers = [
    row for row in parts_suppliers
    if min_cost_dict.get(row[3]) is not None and min_cost_dict[row[3]] == row[-1]
]

# Write the final result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY',
                         'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'])
    csv_writer.writerows(min_cost_suppliers)

print("Query execution complete and results saved to 'query_output.csv'.")

