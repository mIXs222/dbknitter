import csv
import pymysql
import pymongo

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch

# Extract supplier data where S_NATIONKEY corresponds to 'GERMANY' in nation collection
nation_docs = list(mongodb.nation.find({"N_NAME": "GERMANY"}, {"N_NATIONKEY": 1}))
german_nation_keys = [doc["N_NATIONKEY"] for doc in nation_docs]

# Query to select supplier with nation keys for Germany
supplier_sql = f"""
SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY IN ({','.join(map(str, german_nation_keys))})
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(supplier_sql)
    supplier_rows = cursor.fetchall()
german_supplier_keys = [row[0] for row in supplier_rows]

# Query to calculate VALUE per PS_PARTKEY from partSupp of the Germany suppliers
partsupp_docs = mongodb.partsupp.aggregate([
    {
        "$match": {
            "PS_SUPPKEY": {"$in": german_supplier_keys}
        }
    },
    {
        "$group": {
            "_id": "$PS_PARTKEY",
            "VALUE": {
                "$sum": {
                    "$multiply": ["$PS_SUPPLYCOST", "$PS_AVAILQTY"]
                }
            }
        }
    }
])
partsupp_values = list(partsupp_docs)

# Calculate the sum for the subquery condition
total_value = sum(doc['VALUE'] for doc in partsupp_values)
value_threshold = total_value * 0.0001000000

# Filter out partsupp_values below threshold
filtered_partsupp_values = [
    doc for doc in partsupp_values if doc['VALUE'] > value_threshold
]

# Sort by VALUE in descending order
sorted_partsupp_values = sorted(
    filtered_partsupp_values, key=lambda x: x['VALUE'], reverse=True)

# Write to CSV file: query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PS_PARTKEY', 'VALUE'])
    for part in sorted_partsupp_values:
        writer.writerow([part['_id'], part['VALUE']])

# Close connections
mysql_conn.close()
mongo_client.close()
