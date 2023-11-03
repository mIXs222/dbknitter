import mysql.connector
import pymongo
import pandas as pd

# Connection to MySQL
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Fetch data from MySQL
mysql_cursor.execute("SELECT S_SUPPKEY, S_NATIONKEY FROM supplier, nation WHERE S_NATIONKEY = N_NATIONKEY AND N_NAME = 'GERMANY'")
mysql_data = mysql_cursor.fetchall()

suppliers = [row[0] for row in mysql_data if row]
nations = [row[1] for row in mysql_data if row]

# Query MongoDB partsupp
mongo_query = { "$and": [ { "PS_SUPPKEY": { "$in": suppliers } }, { "PS_AVAILQTY": { "$gt": 0 } } ] }
mongo_docs = mongo_db.partsupp.find(mongo_query)

# Prepare data for pandas DataFrame
data = [(doc['PS_PARTKEY'], doc['PS_SUPPLYCOST'] * doc['PS_AVAILQTY']) for doc in mongo_docs]

# Perform groupby operation using pandas
df = pd.DataFrame(data, columns=['PS_PARTKEY', 'VALUE'])
grouped = df.groupby('PS_PARTKEY').sum()

# Filter rows based on the given condition
threshold = grouped['VALUE'].sum() * 0.0001000000
filtered = grouped[grouped['VALUE'] > threshold]

# Sort and save to csv
result = filtered.sort_values('VALUE', ascending=False)
result.to_csv('query_output.csv')
