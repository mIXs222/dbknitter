import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch nation keys for 'GERMANY'
mysql_cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY'")
germany_nation_key = mysql_cursor.fetchone()[0]

# Fetch partsupp information
mysql_cursor.execute("""
    SELECT PS_PARTKEY, PS_AVAILQTY, PS_SUPPLYCOST
    FROM partsupp
    WHERE PS_SUPPKEY IN (
        SELECT S_SUPPKEY FROM supplier WHERE S_NATIONKEY = %s
    )
""", (germany_nation_key,))
partsupp_data = mysql_cursor.fetchall().items()

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']

# Fetch supplier keys for 'GERMANY'
german_suppliers = list(supplier_collection.find({'S_NATIONKEY': germany_nation_key}, {'_id': 0, 'S_SUPPKEY': 1}))
german_supp_keys = [s['S_SUPPKEY'] for s in german_suppliers]

# Filter partsupp data for german suppliers
german_partsupp_data = [row for row in partsupp_data if row['PS_SUPPKEY'] in german_supp_keys]

# Calculate total value and filter significant parts
total_value = sum(row['PS_AVAILQTY'] * row['PS_SUPPLYCOST'] for row in german_partsupp_data)
important_parts = [(row['PS_PARTKEY'], row['PS_AVAILQTY'] * row['PS_SUPPLYCOST']) for row in german_partsupp_data if (row['PS_AVAILQTY'] * row['PS_SUPPLYCOST']) / total_value > 0.0001]

# Sort parts by value in descending order
important_parts.sort(key=lambda x: x[1], reverse=True)

# Write to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['PARTKEY', 'VALUE'])
    for part_key, value in important_parts:
        writer.writerow([part_key, value])
