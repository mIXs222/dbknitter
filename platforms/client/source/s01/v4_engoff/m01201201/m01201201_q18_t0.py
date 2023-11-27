# query.py

import pandas as pd
import pymysql
from pymongo import MongoClient
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongodb_client = MongoClient('mongodb', 27017)
mongodb_database = mongodb_client['tpch']

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Function to get orders from MySQL
def get_mysql_orders():
    with mysql_connection.cursor() as cursor:
        query = """
        SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE
        FROM orders
        WHERE O_TOTALPRICE > 300
        """
        cursor.execute(query)
        return cursor.fetchall()

mysql_orders = get_mysql_orders()

# Function to get lineitems from MongoDB
def get_mongo_lineitems():
    lineitems = mongodb_database["lineitem"]
    return list(lineitems.find({"L_QUANTITY": {"$gt": 300}}))

mongo_lineitems = get_mongo_lineitems()

# Function to get customers from Redis
def get_redis_customers():
    customer_data = redis_connection.get('customer')
    customer_df = pd.read_json(customer_data)
    return customer_df

redis_customers = get_redis_customers()

# Convert MySQL orders to DataFrame
orders_df = pd.DataFrame(mysql_orders, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

# Convert MongoDB lineitems to DataFrame and group by order key to sum quantities
lineitems_df = pd.DataFrame(mongo_lineitems)
lineitems_df = lineitems_df.groupby('L_ORDERKEY').sum().reset_index()

# Merge dataframes to get the result
results = orders_df.merge(redis_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')
results = results.merge(lineitems_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
results = results[['C_NAME', 'O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Write results to csv
results.to_csv('query_output.csv', index=False)
