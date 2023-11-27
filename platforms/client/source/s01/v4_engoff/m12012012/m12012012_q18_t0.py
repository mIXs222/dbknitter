# File: query_code.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()
# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Querying mysql for customers
mysql_cursor.execute('SELECT C_CUSTKEY, C_NAME FROM customer')
customers = pd.DataFrame(mysql_cursor.fetchall(), columns=['C_CUSTKEY', 'C_NAME'])

# Querying mongodb for orders larger than 300 quantity
orders = pd.DataFrame(list(mongodb_db.orders.find({"O_TOTALPRICE": {"$gt": 300}})))

# Querying redis for lineitem, convert bytes to dict
lineitems_list = redis_client.get('lineitem')
lineitems = pd.DataFrame([eval(lineitem.decode("utf-8")) for lineitem in lineitems_list])

# Merging the DataFrames
result = (
    customers.merge(orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
)

# Writing the final DataFrame to CSV
result[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']].to_csv(
    'query_output.csv', index=False
)

# Closing connections
mysql_conn.close()
mongodb_client.close()
