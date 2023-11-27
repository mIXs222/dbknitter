import pandas as pd
import pymysql
import pymongo
import direct_redis
from bson.objectid import ObjectId

# Connect to MySQL for lineitem
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Query for lineitem
mysql_query = """
SELECT L_ORDERKEY, (L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as REVENUE
FROM lineitem
WHERE L_SHIPDATE > '1995-03-15'
"""
mysql_cursor.execute(mysql_query)

# Fetch the data from lineitem
lineitem_data = mysql_cursor.fetchall()
lineitem_df = pd.DataFrame(list(lineitem_data), columns=['L_ORDERKEY', 'REVENUE'])
mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB for customer
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_customer = mongo_db['customer']

# Query for customer
customer_docs = mongo_customer.find({'C_MKTSEGMENT': 'BUILDING'})
customer_df = pd.DataFrame(list(customer_docs), columns=['_id', 'C_CUSTKEY'])

# Connect to Redis for orders
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch the data from orders
orders_data = redis_conn.get('orders')
orders_df = pd.read_json(orders_data, orient='records') if orders_data else pd.DataFrame()

# Merge dataframes
merged_df = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filtering and sorting
result_df = merged_df.groupby('O_ORDERKEY').agg({'REVENUE': 'sum', 'O_SHIPPRIORITY': 'first'}).reset_index()
result_df = result_df.sort_values(by='REVENUE', ascending=False)

# Writing to CSV
result_df.to_csv('query_output.csv', index=False)
