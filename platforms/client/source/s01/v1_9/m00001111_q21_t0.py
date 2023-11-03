from pymongo import MongoClient
import mysql.connector
import pandas as pd

# SQL database connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql', database='tpch')
cursor = cnx.cursor()

# MongoDB connection
client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]
partsupp = db.partsupp

# Query MySQL
query = ("SELECT S_NAME, S_NATIONKEY FROM supplier WHERE S_NATIONKEY IN "
         "(SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'SAUDI ARABIA')")

cursor.execute(query)

suppliers = []
for (S_NAME, S_NATIONKEY) in cursor:
    suppliers.append({'S_NAME': S_NAME, 'S_NATIONKEY': S_NATIONKEY})

# Query MongoDB
lineitem = db.lineitem.find({
                    'L_SUPPKEY': {'$in': [s['S_SUPPKEY'] for s in suppliers]}, 
                    'L_RECEIPTDATE': { '$gt': 'L_COMMITDATE' }
                }, {'L_SUPPKEY': 1} )

# Aggregate the data
result = {}
for item in lineitem:
    suppkey = item['L_SUPPKEY']
    supplier = next((s for s in suppliers if s['S_SUPPKEY'] == suppkey), None)

    if supplier['S_NAME'] not in result:
        result[supplier['S_NAME']] = 0
    result[supplier['S_NAME']] += 1

# Write output 
df = pd.DataFrame(result.items(), columns=['S_NAME', 'NUMWAIT'])
