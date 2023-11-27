import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query for MySQL to retrieve lineitem data
mysql_query = """
SELECT L_ORDERKEY, SUM(L_QUANTITY) as TOTAL_QUANTITY
FROM lineitem
GROUP BY L_ORDERKEY
HAVING TOTAL_QUANTITY > 300
"""
lineitems_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_customers = mongo_db["customer"]

# Get customer data from MongoDB
customers_df = pd.DataFrame(list(mongo_customers.find({})))

# Connection to Redis
redis_conn = DirectRedis(host="redis", port=6379, db=0)

# Get orders data from Redis and convert to DataFrame
orders_df = pd.read_json(redis_conn.get('orders'))

# Filter lineitems by matched order keys from Redis orders
large_orders = lineitems_df[lineitems_df['L_ORDERKEY'].isin(orders_df['O_ORDERKEY'])]

# Merge dataframes to match condition and select required columns
result = pd.merge(
    left=large_orders,
    right=orders_df[['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_TOTALPRICE']],
    how='inner',
    left_on='L_ORDERKEY',
    right_on='O_ORDERKEY'
)

final_result = pd.merge(
    left=result,
    right=customers_df[['C_CUSTKEY', 'C_NAME']],
    how='inner',
    left_on='O_CUSTKEY',
    right_on='C_CUSTKEY'
)[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY']]

# Write to CSV file
final_result.to_csv('query_output.csv', index=False)
