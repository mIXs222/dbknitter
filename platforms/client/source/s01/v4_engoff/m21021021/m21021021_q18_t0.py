import pymysql
import pymongo
import pandas as pd
import direct_redis
from pymongo import MongoClient

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['lineitem']

# Connect to Redis using direct_redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query MongoDB for large quantity orders line items
pipeline = [
    {"$match": {"L_QUANTITY": {"$gt": 300}}},
    {"$project": {"_id": 0, "L_ORDERKEY": 1, "L_QUANTITY": 1}}
]
large_quantity_orders = list(mongo_collection.aggregate(pipeline))

# Create a DataFrame for large quantity orders from MongoDB
mongo_df = pd.DataFrame(large_quantity_orders)
mongo_df.columns = ['O_ORDERKEY', 'L_QUANTITY']  # Rename columns to match with Redis naming convention

# Query Redis for orders data and create DataFrame
orders_data = redis_conn.get('orders')
orders_df = pd.read_json(orders_data, orient='records') if orders_data else pd.DataFrame()

# Merge MongoDB and Redis DataFrames
merged_df = pd.merge(orders_df, mongo_df, on='O_ORDERKEY', how='inner')

# Now, Query MySQL for customer data
mysql_query = """
SELECT C_CUSTKEY, C_NAME FROM customer
"""
mysql_cursor.execute(mysql_query)
customer_data = mysql_cursor.fetchall()

# Create a DataFrame for customers from MySQL
customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NAME'])

# Merge all data together
final_df = pd.merge(merged_df, customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')
final_df = final_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
final_df.columns = ['Customer Name', 'Customer Key', 'Order Key', 'Date', 'Total Price', 'Quantity']
final_df.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
redis_conn.close()
mongo_client.close()
