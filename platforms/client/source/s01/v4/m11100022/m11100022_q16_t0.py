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
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']

# Retrieve supplier keys with the specified comment from MySQL
supplier_keys_query = """
SELECT S_SUPPKEY
FROM supplier
WHERE S_COMMENT LIKE '%Customer%Complaints%'
"""
mysql_cursor.execute(supplier_keys_query)
excluded_supplier_keys = [row[0] for row in mysql_cursor.fetchall()]

# Retrieve partsupp with the specified conditions from MySQL
partsupp_query = """
SELECT 
    PS_PARTKEY, 
    PS_SUPPKEY
FROM partsupp
WHERE PS_SUPPKEY NOT IN %s
"""
mysql_cursor.execute(partsupp_query, (excluded_supplier_keys,))
partsupp_records = mysql_cursor.fetchall()

# Convert to a dictionary for efficient lookup
partsupp_dict = {}
for ps_partkey, ps_suppkey in partsupp_records:
    if ps_partkey not in partsupp_dict:
        partsupp_dict[ps_partkey] = set()
    partsupp_dict[ps_partkey].add(ps_suppkey)

# Retrieve part with the specified conditions from MongoDB
part_query = {
    "P_BRAND": {"$ne": 'Brand#45'},
    "P_TYPE": {"$not": {"$regex": '^MEDIUM POLISHED'}},
    "P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}
}
part_records = mongo_db.part.find(part_query)

# Aggregate results
result_dict = {}
for part_record in part_records:
    p_partkey = part_record["P_PARTKEY"]
    if p_partkey in partsupp_dict:
        brand_type_size = (
            part_record["P_BRAND"],
            part_record["P_TYPE"],
            part_record["P_SIZE"]
        )
        supp_cnt = len(partsupp_dict[p_partkey])
        if brand_type_size not in result_dict:
            result_dict[brand_type_size] = supp_cnt
        else:
            result_dict[brand_type_size] += supp_cnt

# Sort and write results to CSV
sorted_results = sorted(
    result_dict.items(), 
    key=lambda item: (-item[1], item[0])
)
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(["P_BRAND", "P_TYPE", "P_SIZE", "SUPPLIER_CNT"])
    for (p_brand, p_type, p_size), supplier_cnt in sorted_results:
        writer.writerow([p_brand, p_type, p_size, supplier_cnt])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
