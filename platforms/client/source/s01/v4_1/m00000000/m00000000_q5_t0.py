import pandas as pd
import pymysql
import pymongo
import redis
import direct_redis
import csv

# MySQL Connection
db = pymysql.connect("mysql","root","my-secret-pw","tpch")
cursor = db.cursor()

# MongoDB Connection
client = pymongo.MongoClient("mongodb://root:my-secret-pw@mongo/tpch")
db_mongo = client['tpch']

# Redis Connection
redis_instance = redis.StrictRedis(host='redis', port=6379, password='my-secret-pw')

# Defining Query
sql_query = '''SELECT
            N_NAME,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
        FROM
            customer,
            orders,
            lineitem,
            supplier,
            nation,
            region
        WHERE
            C_CUSTKEY = O_CUSTKEY
            AND L_ORDERKEY = O_ORDERKEY
            AND L_SUPPKEY = S_SUPPKEY
            AND C_NATIONKEY = S_NATIONKEY
            AND S_NATIONKEY = N_NATIONKEY
            AND N_REGIONKEY = R_REGIONKEY
            AND R_NAME = 'ASIA'
            AND O_ORDERDATE >= '1990-01-01'
            AND O_ORDERDATE < '1995-01-01'
        GROUP BY
            N_NAME
        ORDER BY
            REVENUE DESC'''

# Get data from MySQL
cursor.execute(sql_query)
mysql_data = cursor.fetchall()
mysql_df = pd.DataFrame(mysql_data, columns = ['N_NAME','REVENUE'])

# Get data from MongoDB
collection_list = db_mongo.list_collection_names()
mongo_data = []
for collection in collection_list:
    mongo_data.extend(list(db_mongo[collection].find()))
mongo_df = pd.DataFrame(mongo_data)

# Get data from Redis
keys = redis_instance.keys()
redis_data = [direct_redis.get(key) for key in keys]
redis_df = pd.concat(redis_data, ignore_index=True)

# Combine the Data
combined_df = pd.concat([mysql_df, mongo_df, redis_df], ignore_index=True)

# Write to csv
combined_df.to_csv('query_output.csv', quoting=csv.QUOTE_NONNUMERIC)
