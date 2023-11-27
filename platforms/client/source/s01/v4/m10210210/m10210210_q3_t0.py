import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient
import redis
import direct_redis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_orders_collection = mongo_db['orders']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL (lineitem table)
mysql_cursor.execute("""
    SELECT 
        L_ORDERKEY, 
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_SHIPDATE 
    FROM 
        lineitem
""")
lineitems = mysql_cursor.fetchall()
lineitem_df = pd.DataFrame(lineitems, columns=['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPDATE'])

# Fetch data from MongoDB (orders table)
mongo_orders_query = { "O_ORDERDATE": { "$lt": "1995-03-15" } }
mongo_orders_projection = {
    "_id": 0,
    "O_ORDERKEY": 1,
    "O_CUSTKEY": 1,
    "O_ORDERDATE": 1,
    "O_SHIPPRIORITY": 1
}
orders_df = pd.DataFrame(list(mongo_orders_collection.find(mongo_orders_query, mongo_orders_projection)))

# Fetch data from Redis (customer table)
customer_str = redis_client.get('customer')
customer_df = pd.read_json(customer_str)

# Filtering customers with MARKETSEGMENT 'BUILDING'
customer_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

# Joining dataframes
joined_df = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
joined_df = joined_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filtering based on the date conditions
joined_df = joined_df[joined_df['L_SHIPDATE'] > '1995-03-15']

# Group by and aggregate
result_df = joined_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg(
    REVENUE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: sum(x * (1 - joined_df.loc[x.index, 'L_DISCOUNT'])))
).reset_index()

# Sorting the results
result_df = result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mysql_conn.close()
mongo_client.close()
