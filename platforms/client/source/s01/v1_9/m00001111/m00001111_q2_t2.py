import mysql.connector
import pymongo
import csv
from bson.son import SON

# Mysql Connection
mydb = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

mycursor = mydb.cursor()

# MongoDB Connection
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client.tpch

# Perform mysql query
mysql_query = """
SELECT S_ACCTBAL, S_NAME, N_NAME, S_ADDRESS, S_PHONE, S_COMMENT
FROM supplier, nation, region
WHERE S_NATIONKEY = N_NATIONKEY
AND N_REGIONKEY = R_REGIONKEY
AND R_NAME = 'EUROPE'
ORDER BY S_ACCTBAL DESC, N_NAME, S_NAME;
"""

mycursor.execute(mysql_query)

mysql_result = mycursor.fetchall()

# MongoDB query for part and partsupp
pipeline = [
    {'$match': {
        'P_SIZE': 15, 
        'P_TYPE': {'$regex': 'BRASS'},
        'PS_SUPPLYCOST': {
            '$eq': [ {
                '$group': {
                    '_id': None,
                    'minCost': {'$min': '$PS_SUPPLYCOST'}
                }
            }]
        }
    }},
    {'$project': {
        '_id': 0,
        'P_PARTKEY': 1,
        'P_MFGR': 1,
        'PS_PARTKEY': 1,
    }}
]

mongodb_result = mongodb.partsupp.aggregate(pipeline)

mongodb_result = list(map(lambda x: list(x.values()), mongodb_result))

# Join mysql result and mongodb result on partkey and write to csv
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['S_ACCTBAL', 'S_NAME', 'N_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_MFGR', 'P_PARTKEY'])
    for row in mysql_result:
        for item in mongodb_result:
            if(row[0] == item[0]):
                writer.writerow(list(row) + list(item))
