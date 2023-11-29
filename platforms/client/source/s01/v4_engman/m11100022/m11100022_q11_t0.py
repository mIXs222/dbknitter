# import_libraries.py
import csv
import pymysql
import pymongo

# Connect to MySQL (supplier and partsupp tables)
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cur = conn.cursor()

# Connect to MongoDB (nation table)
from pymongo import MongoClient
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']
nation_collection = mongodb['nation']

# Find nationkey for GERMANY
germany_nationkey = nation_collection.find_one({'N_NAME': 'GERMANY'})['N_NATIONKEY']

# Execute query for partsupp and supplier tables in MySQL
cur.execute("""SELECT PS_PARTKEY, SUM(PS_AVAILQTY * PS_SUPPLYCOST) AS value 
               FROM partsupp 
               JOIN supplier ON PS_SUPPKEY = S_SUPPKEY 
               WHERE S_NATIONKEY = %s 
               GROUP BY PS_PARTKEY 
               HAVING SUM(PS_AVAILQTY * PS_SUPPLYCOST) > 0.0001 
               ORDER BY value DESC""", (germany_nationkey,))

results = cur.fetchall()

# Write query result to file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['PS_PARTKEY', 'VALUE'])  # header
    for row in results:
        csvwriter.writerow(row)

# Close the database connections
cur.close()
conn.close()
client.close()
