import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from pymongo.collection import Collection

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connection to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = Collection(mongodb_db, 'lineitem')

# Connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get orders from MySQL where O_ORDERDATE < '1995-03-05'
mysql_query = """
SELECT O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
FROM orders
WHERE O_ORDERDATE < '1995-03-05'
"""
mysql_cursor.execute(mysql_query)
orders_data = mysql_cursor.fetchall()
orders_df = pd.DataFrame(orders_data, columns=['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])

# Get lineitem from MongoDB where L_SHIPDATE > '1995-03-15'
lineitem_query = {
    'L_SHIPDATE': {'$gt': '1995-03-15'}
}
lineitem_projection = {
    'L_ORDERKEY': 1,
    'L_EXTENDEDPRICE': 1,
    'L_DISCOUNT': 1
}
lineitems_cursor = lineitem_collection.find(lineitem_query, lineitem_projection)
lineitems_df = pd.DataFrame(list(lineitems_cursor))

# Calculate REVENUE
lineitems_df['REVENUE'] = lineitems_df['L_EXTENDEDPRICE'] * (1 - lineitems_df['L_DISCOUNT'])

# Get customer from Redis
customers_data = redis_conn.get('customer')
customers_df = pd.read_json(customers_data, orient='records')

# Filter customers where C_MKTSEGMENT is 'BUILDING'
customers_df = customers_df[customers_df['C_MKTSEGMENT'] == 'BUILDING']

# Merge all dataframes
merged_df = orders_df.merge(lineitems_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = merged_df.merge(customers_df, left_on='O_ORDERKEY', right_on='C_CUSTKEY')

# Select and sort the data as per requirements
result_df = merged_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]
result_df = result_df.sort_values(by='REVENUE', ascending=False)

# Write result to a csv file
result_df.to_csv('query_output.csv', index=False)

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
