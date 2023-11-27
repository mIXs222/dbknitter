import pymysql
import pymongo
import csv

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query the MySQL 'partsupp' table
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute('''
            SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST, PS_AVAILQTY
            FROM partsupp;
        ''')
        partsupp_data = cursor.fetchall()

# Query the MongoDB 'supplier' and 'nation' collections
suppliers = mongo_db['supplier'].find()
nations = mongo_db['nation'].find({'N_NAME': 'GERMANY'})

# Join 'supplier' with 'nation' on S_NATIONKEY = N_NATIONKEY and then create a dictionary for quick lookup
nation_supplier_map = {
    supplier['S_SUPPKEY']: supplier for nation in nations for supplier in suppliers if supplier['S_NATIONKEY'] == nation['N_NATIONKEY']
}

# Process the data and perform group by and having operations in Python
grouped_data = {}
for part in partsupp_data:
    ps_partkey, ps_suppkey, ps_supplycost, ps_availqty = part
    if ps_suppkey in nation_supplier_map:
        grouped_data_key = ps_partkey
        value = ps_supplycost * ps_availqty
        if grouped_data_key not in grouped_data:
            grouped_data[grouped_data_key] = 0
        grouped_data[grouped_data_key] += value

# Filtering based on the having condition
threshold_query = sum(grouped_data.values()) * 0.0001000000
filtered_data = {k: v for k, v in grouped_data.items() if v > threshold_query}

# Sorting the data by VALUE in descending order
sorted_filtered_data = sorted(filtered_data.items(), key=lambda item: item[1], reverse=True)

# Write the output to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PS_PARTKEY', 'VALUE'])
    for data in sorted_filtered_data:
        writer.writerow(data)

# Close connections
mysql_conn.close()
mongo_client.close()
