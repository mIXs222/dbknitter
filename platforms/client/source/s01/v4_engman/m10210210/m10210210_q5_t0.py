# file: query_code.py
import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query to select data from region and lineitem tables in MySQL
mysql_query = """
SELECT r.R_NAME AS N_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE
FROM region r
JOIN nation n ON r.R_REGIONKEY = n.N_REGIONKEY
JOIN supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
JOIN lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
WHERE r.R_NAME = 'ASIA'
  AND l.L_SHIPDATE BETWEEN '1990-01-01' AND '1995-01-01'
GROUP BY n.N_NAME
ORDER BY REVENUE DESC;
"""

mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation_collection = mongo_db["nation"]

# Query to select nation keys from ASIA region in MongoDB
mongo_results = nation_collection.find({"N_REGIONKEY": {"$in": [asia_region_key_from_sql]}})
nation_keys = [doc["N_NATIONKEY"] for doc in mongo_results]

# Connect to Redis using direct_redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
customer_df = pd.DataFrame(eval(redis_conn.get('customer')))

# Filter customers based in Asia
asian_customers = customer_df[customer_df['C_NATIONKEY'].isin(nation_keys)]

# Now we need to combine the data from MySQL and Redis based on the customers

# First we get the total volume per nation
nation_volumes = mysql_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Then we get the total volume from customers and merge the data
customer_volumes = asian_customers.groupby('C_NATIONKEY')['REVENUE'].sum().reset_index()
final_result = nation_volumes.merge(customer_volumes, left_on='N_NAME', right_on='C_NATIONKEY', how='left')

# Save result to CSV
final_result.to_csv('query_output.csv', index=False)
