# suppliers_query.py
import pymysql
import pymongo
import pandas as pd
import direct_redis

# MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# MongoDB connection parameters
mongodb_params = {
    'host': 'mongodb',
    'port': 27017,
}

# Redis connection parameters
redis_params = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to MySQL server
mysql_connection = pymysql.connect(**mysql_params)
try:
    with mysql_connection.cursor() as cursor:
        # Define and execute the MySQL query
        mysql_query = """
        SELECT s.S_SUPPKEY, s.S_NAME
        FROM supplier as s
        WHERE s.S_NATIONKEY = (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'SAUDI ARABIA')
        """
        cursor.execute(mysql_query)
        supplier_results = cursor.fetchall()
finally:
    mysql_connection.close()

# Connect to MongoDB server
mongo_client = pymongo.MongoClient(**mongodb_params)
mongodb = mongo_client['tpch']
try:
    # Query MongoDB for relevant documents
    orders_query = {'O_ORDERSTATUS': 'F'}
    orders = list(mongodb.orders.find(orders_query))
finally:
    mongo_client.close()

# Connect to Redis server using direct_redis
r = direct_redis.DirectRedis(**redis_params)
try:
    # Get the 'nation' table from Redis and convert it to a pandas DataFrame
    nation_data = r.get('nation')
    nation_df = pd.read_json(nation_data)
finally:
    r.close()

# Convert query results to pandas DataFrames
supplier_df = pd.DataFrame(list(supplier_results), columns=['S_SUPPKEY', 'S_NAME'])

# Process lineitem information from MongoDB
lineitem_df = pd.DataFrame.from_records(mongodb.lineitem.find())

# Filtering the orders where suppliers failed to meet the delivery date
lineitem_df = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]

# Find which supplier was the only one late in fulfilling an order
late_supplier_by_order = lineitem_df.groupby('L_ORDERKEY').filter(
    lambda group: (group['L_SUPPKEY'].isin(supplier_df['S_SUPPKEY']) & group['L_RECEIPTDATE'] > group['L_COMMITDATE']).sum() == 1
)
late_supplier_orders = late_supplier_by_order['L_ORDERKEY'].unique()

# Filter those suppliers whose products were part of the late orders
late_suppliers = supplier_df[supplier_df['S_SUPPKEY'].isin(late_supplier_by_order['L_SUPPKEY'])]

# Filter those orders that have only one late supplier
unique_late_suppliers_orders = orders[orders['O_ORDERKEY'].isin(late_supplier_orders)]

# Final results
final_results = late_suppliers[['S_SUPPKEY', 'S_NAME']].drop_duplicates()

# Write the output to a CSV file
final_results.to_csv('query_output.csv', index=False)
