import pymysql
import pymongo
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection setup
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
partsupp_collection = mongo_db['partsupp']

# SQL query to select all from part and supplier tables where conditions are met
sql = """
    SELECT p.P_BRAND, p.P_TYPE, p.P_SIZE, COUNT(DISTINCT s.S_SUPPKEY) AS supplier_count
    FROM part p 
    JOIN supplier s ON p.P_PARTKEY = s.S_SUPPKEY 
    WHERE p.P_BRAND <> 'Brand#45' AND p.P_TYPE NOT LIKE 'MEDIUM POLISHED%' 
            AND p.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9) 
            AND s.S_COMMENT NOT LIKE '%Customer%Complaints%'
    GROUP BY p.P_BRAND, p.P_TYPE, p.P_SIZE
    ORDER BY supplier_count DESC, p.P_BRAND ASC, p.P_TYPE ASC, p.P_SIZE ASC;
"""

# Execute SQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(sql)
    part_supplier_results = cursor.fetchall()

# Function to check if a supplier can supply a part with a specific partkey
def can_supply(p_partkey):
    return partsupp_collection.find_one({'PS_PARTKEY': p_partkey}) is not None

# Filter results based on partsupp information from MongoDB
filtered_results = [
    (p_brand, p_type, p_size, supplier_count) 
    for p_brand, p_type, p_size, supplier_count in part_supplier_results
    if can_supply({'P_PARTKEY': partsupp_collection})
]

# Write the results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["P_BRAND", "P_TYPE", "P_SIZE", "SUPPLIER_COUNT"])
    for row in filtered_results:
        writer.writerow(row)

# Closing connections
mysql_conn.close()
mongo_client.close()
