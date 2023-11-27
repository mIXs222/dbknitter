# query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', 
                             user='root', 
                             password='my-secret-pw', 
                             database='tpch')

# Fetch Order and Lineitem tables from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM orders WHERE O_ORDERSTATUS = 'F'")
    orders = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

    cursor.execute("SELECT * FROM lineitem WHERE L_COMMITDATE < L_RECEIPTDATE")
    lineitem = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_nation = mongo_db["nation"]
nation = pd.DataFrame(list(mongo_nation.find({"N_NAME": "SAUDI ARABIA"})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier_data = redis_client.get('supplier')
supplier = pd.read_json(supplier_data, orient='records')

# Preprocessing
orders.set_index('O_ORDERKEY', inplace=True)
lineitem.set_index('L_ORDERKEY', inplace=True)
nation.set_index('N_NATIONKEY', inplace=True)
supplier.set_index('S_NATIONKEY', inplace=True)

# Perform the join operation
result = lineitem.join(orders, how='inner')
result = result.join(supplier, on='L_SUPPKEY', how='inner', rsuffix='_SUPPLIER')
result = result.join(nation, on='S_NATIONKEY', rsuffix='_NATION')
final_result = result[result['N_NAME'] == 'SAUDI ARABIA'].drop_duplicates(subset='L_ORDERKEY', keep=False)

# Output to CSV
final_result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
