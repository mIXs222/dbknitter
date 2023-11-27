import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Fetch nation and supplier from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT 
            n.N_NATIONKEY, s.S_SUPPKEY, s.S_NAME, s.S_COMMENT,
            s.S_ACCTBAL, ps.PS_PARTKEY, ps.PS_SUPPLYCOST
        FROM 
            nation n
            JOIN supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
            JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
        WHERE 
            n.N_NAME = 'GERMANY'
    """)
    
    mysql_data = cursor.fetchall()

# Fetch partsupp from MongoDB
partsupp_collection = mongodb_db['partsupp']
mongodb_data = list(partsupp_collection.find())

# Calculate total value of all available parts in Germany
total_value = sum(rec['PS_SUPPLYCOST'] * rec['PS_AVAILQTY'] for rec in mongodb_data)

# Find parts that represent a significant percentage of the total value
important_parts = [
    (rec['PS_PARTKEY'], rec['PS_SUPPLYCOST'] * rec['PS_AVAILQTY']) 
    for rec in mongodb_data
    if rec['PS_SUPPLYCOST'] * rec['PS_AVAILQTY'] > total_value * 0.0001
]

# Sort the parts in descending order of value
important_parts.sort(key=lambda x: x[1], reverse=True)

# Combine data from MySQL and MongoDB
final_output = []
for s_nationkey, s_suppkey, s_name, s_comment, s_acctbal, ps_partkey, ps_supplycost in mysql_data:
    for part, value in important_parts:
        if s_suppkey == part:
            final_output.append((ps_partkey, value))

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['Part Number', 'Value'])
    for part_number, value in final_output:
        csv_writer.writerow([part_number, value])

# Close connections
mysql_conn.close()
mongodb_client.close()
