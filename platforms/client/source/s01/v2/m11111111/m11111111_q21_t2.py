from pymongo import MongoClient
import pandas as pd
import pymysql.cursors

# MongoDB settings
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['tpch']
collection_names = ['supplier', 'lineitem', 'orders', 'nation']

mongo_data = {}
for collection_name in collection_names:
    if collection_name in mongo_db.list_collection_names():
        mongo_data[collection_name] = pd.DataFrame(list(mongo_db[collection_name].find()))

# MySQL settings -- just a placeholder
mysql_conn = pymysql.connect(host='localhost',
                             user='user',
                             password='password',
                             db='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with mysql_conn.cursor() as cursor:
        for collection_name in collection_names:
            sql = f"SELECT * FROM {collection_name}"
            cursor.execute(sql)

            mysql_data[collection_name] = pd.DataFrame(cursor.fetchall())
finally:
    mysql_conn.close()

# TODO: perform the querying steps using pandas DataFrame here. This step needs a lot of coding.
# df = pd.merge(...) 

df.to_csv('query_output.csv')
