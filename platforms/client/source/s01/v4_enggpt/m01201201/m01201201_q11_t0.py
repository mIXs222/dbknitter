import csv
import pymysql
import pymongo

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
partsupp_collection = mongodb_db['partsupp']

# Get German suppliers from MySQL
mysql_cursor.execute("""
SELECT N_NATIONKEY, S_SUPPKEY
FROM nation, supplier
WHERE nation.N_NATIONKEY = supplier.S_NATIONKEY AND N_NAME = 'GERMANY';
""")
german_suppliers = mysql_cursor.fetchall()

# Get partsupp data from MongoDB
all_partsupps = partsupp_collection.find({})

# Combine and analyze the data
results = []
for ps in all_partsupps:
    for nationkey, suppkey in german_suppliers:
        if ps['PS_SUPPKEY'] == suppkey:
            value = ps['PS_SUPPLYCOST'] * ps['PS_AVAILQTY']
            results.append((ps['PS_PARTKEY'], value))

# Calculate threshold
threshold_value = sum(value for _, value in results) * 0.05  # Assuming 5% for this example

# Filter based on threshold
filtered_results = [result for result in results if result[1] > threshold_value]

# Sort the results by value in descending order
final_results = sorted(filtered_results, key=lambda x: x[1], reverse=True)

# Write results to CSV
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['PS_PARTKEY', 'VALUE'])
    for result in final_results:
        writer.writerow(result)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
