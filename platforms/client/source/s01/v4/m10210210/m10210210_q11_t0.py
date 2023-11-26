# query.py
import csv
import pymysql
import pymongo

# Connection to MySQL database
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch
supplier_collection = mongodb.supplier
nation_collection = mongodb.nation

# Query MySQL to get partsupp data
with mysql_connection.cursor() as cursor:
    partsupp_query = """
    SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST, PS_AVAILQTY
    FROM partsupp
    """
    cursor.execute(partsupp_query)
    partsupp_data = cursor.fetchall()

# Transforming the MongoDB documents to look like SQL table rows.
supplier_query = {"S_NATIONKEY": {"$exists": True}}
supplier_data = {doc['S_SUPPKEY']: doc for doc in supplier_collection.find(supplier_query)}
nation_query = {"N_NAME": "GERMANY"}
nation_data = {doc['N_NATIONKEY']: doc for doc in nation_collection.find(nation_query)}

# Performing the join and aggregation
results = {}
for (ps_partkey, ps_suppkey, ps_supplycost, ps_availqty) in partsupp_data:
    if ps_suppkey in supplier_data and supplier_data[ps_suppkey]['S_NATIONKEY'] in nation_data:
        if ps_partkey not in results:
            results[ps_partkey] = 0
        results[ps_partkey] += ps_supplycost * ps_availqty

# Calculate the threshold
threshold = sum(results.values()) * 0.0001000000

# Filtering the results above the threshold
filtered_results = {pk: value for pk, value in results.items() if value > threshold}

# Writing the output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['PS_PARTKEY', 'VALUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for ps_partkey, value in sorted(filtered_results.items(), key=lambda item: item[1], reverse=True):
        writer.writerow({'PS_PARTKEY': ps_partkey, 'VALUE': value})

# Close the connections
mysql_connection.close()
mongo_client.close()
