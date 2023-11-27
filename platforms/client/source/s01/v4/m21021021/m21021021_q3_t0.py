# query.py

import pandas as pd
import pymysql
import pymongo
from direct_redis import DirectRedis

# Connect to MySQL server
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB server
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Connect to Redis server
redis = DirectRedis(host='redis', port=6379, db=0)

# Execute query on MySQL server
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT
            C_CUSTKEY,
            C_NAME
        FROM
            customer
        WHERE
            C_MKTSEGMENT = 'BUILDING'
    """)
    mysql_customers = cursor.fetchall()

# Convert MySQL result to DataFrame
df_customers = pd.DataFrame(mysql_customers, columns=['C_CUSTKEY', 'C_NAME'])

# Fetch data from MongoDB
mongo_lineitems = mongo_db.lineitem.find({
    'L_SHIPDATE': {'$gt': '1995-03-15'}
})

# Convert MongoDB result to DataFrame
df_lineitems = pd.DataFrame(list(mongo_lineitems))

# Fetch and decode data from Redis
redis_orders_raw = redis.get('orders')
df_orders = pd.read_msgpack(redis_orders_raw)

# Filter DataFrame from Redis
df_orders = df_orders[df_orders['O_ORDERDATE'] < '1995-03-15']

# Join dataframes
result = pd.merge(df_customers, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result = pd.merge(result, df_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Perform the grouping and aggregation
result = result.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg(
    REVENUE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: (x * (1 - result['L_DISCOUNT'])).sum())
).reset_index()

# Sort the results
result = result.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write results to CSV
result.to_csv('query_output.csv', index=False)

# Closing connections
mysql_conn.close()
mongo_client.close()
