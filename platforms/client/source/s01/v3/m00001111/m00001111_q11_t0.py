import mysql.connector
import pymongo
import pandas as pd

# MySQL Setup
mysql_db = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)
mysql_cursor = mysql_db.cursor()

# MongoDB setup
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Retrieve data from MySQL and MongoDB
mysql_cursor.execute("SELECT * FROM supplier, nation WHERE S_NATIONKEY = N_NATIONKEY AND N_NAME = 'GERMANY'")
mysql_data = mysql_cursor.fetchall()

mongo_data = mongo_db.partsupp.find()

# Merge MySQL and MongoDB data
data = []
for row in mysql_data:
    for doc in mongo_data:
        if row['S_SUPPKEY'] == doc['PS_SUPPKEY']:
            data.append({
                'PS_PARTKEY': doc['PS_PARTKEY'],
                'VALUE': doc['PS_SUPPLYCOST'] * doc['PS_AVAILQTY']
            })

df = pd.DataFrame(data)

# Perform the query
result = df.groupby('PS_PARTKEY').sum().sort_values(by='VALUE', ascending=False)
threshold = df.VALUE.sum() * 0.0001
result = result[result.VALUE > threshold]

# Write the result to a csv file
result.to_csv('query_output.csv')
