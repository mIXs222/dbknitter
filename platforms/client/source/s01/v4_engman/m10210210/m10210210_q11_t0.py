import pymysql
import pymongo
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# MongoDB connection setup
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']

# Perform MySQL query to get all partsupp information
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT PS_PARTKEY, PS_AVAILQTY, PS_SUPPLYCOST FROM partsupp")
    partsupp_data = cursor.fetchall()

# Close the MySQL connection
mysql_conn.close()

# Get the nation N_NATIONKEY for 'GERMANY'
nation_key = mongodb.nation.find_one({'N_NAME': 'GERMANY'}, {'N_NATIONKEY': 1})['N_NATIONKEY']

# Perform the MongoDB query to get all supplier information in GERMANY
suppliers_germany = list(mongodb.supplier.find({'S_NATIONKEY': nation_key}, {'S_SUPPKEY': 1}))

# Extract the supplier keys for GERMANY
suppliers_keys_germany = [s['S_SUPPKEY'] for s in suppliers_germany]

# Filter partsupp_data to keep only parts supplied by suppliers from GERMANY
filtered_partsupp_data = [row for row in partsupp_data if row[1] in suppliers_keys_germany]

# Calculate the total value and determine significant parts
total_value = sum([qty * cost for _, qty, cost in filtered_partsupp_data])
significant_parts = [(part_key, qty * cost) for part_key, qty, cost in filtered_partsupp_data if qty * cost > total_value * 0.0001]

# Sort significant parts by value in descending order
significant_parts.sort(key=lambda x: x[1], reverse=True)

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['PS_PARTKEY', 'VALUE'])

    for part_key, value in significant_parts:
        csvwriter.writerow([part_key, value])
