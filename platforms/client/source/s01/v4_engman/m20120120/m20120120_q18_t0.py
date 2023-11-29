import pymysql
import pymongo
import pandas as pd
import numpy as np
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()
# Extracting relevant data from lineitem table in MySQL
mysql_query = '''
    SELECT L_ORDERKEY, SUM(L_QUANTITY) AS TOTAL_QUANTITY, L_EXTENDEDPRICE
    FROM lineitem
    GROUP BY L_ORDERKEY
    HAVING SUM(L_QUANTITY) > 300
'''
mysql_cursor.execute(mysql_query)
lineitems_for_large_orders = {
    order_key: (quantity, price) for order_key, quantity, price in mysql_cursor
}
mysql_cursor.close()
mysql_conn.close()


# MongoDB connection
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]
customer_collection = mongodb_db["customer"]
# Extracting customer data from MongoDB
customers = {
    doc['C_CUSTKEY']: doc['C_NAME'] for doc in customer_collection.find({}, {'_id': 0, 'C_CUSTKEY': 1, 'C_NAME': 1})
}
mongodb_client.close()

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)
# Extracting order data from Redis
orders_raw = redis_conn.get('orders')
if orders_raw is not None:
    orders_df = pd.read_pickle(np.loads(orders_raw), compression=None)
else:
    orders_df = pd.DataFrame()
# Filtering for orders present in MySQL lineitem result
orders_df = orders_df[orders_df['O_ORDERKEY'].isin(lineitems_for_large_orders.keys())]
# Adding customer name and total quantity to orders
orders_df['C_NAME'] = orders_df['O_CUSTKEY'].map(customers)
orders_df['TOTAL_QUANTITY'] = orders_df['O_ORDERKEY'].map(lambda key: lineitems_for_large_orders[key][0])
orders_df['L_EXTENDEDPRICE'] = orders_df['O_ORDERKEY'].map(lambda key: lineitems_for_large_orders[key][1])
# Selecting and ordering final result
result_df = orders_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])
result_df = result_df[['C_NAME', 'O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY']]

# Writing to query_output.csv
result_df.to_csv('query_output.csv', index=False)
