import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# SQL Query for the MySQL tables
mysql_query = """
SELECT 
    n.N_NAME as nation,
    YEAR(l.L_SHIPDATE) as year,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) - ps.PS_SUPPLYCOST * l.L_QUANTITY) AS profit
FROM 
    lineitem l
JOIN 
    partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
JOIN 
    (SELECT DISTINCT N_NATIONKEY, N_NAME FROM nation) n ON ps.PS_SUPPKEY = n.N_NATIONKEY
GROUP BY 
    n.N_NAME, YEAR(l.L_SHIPDATE)
"""

# Execute MySQL query
mysql_df = pd.read_sql(mysql_query, mysql_conn)

# Retrieve data from MongoDB
part_conditions = {'P_NAME': {'$regex': 'specified dim'}}
mongo_df = pd.DataFrame(list(mongo_db.part.find(part_conditions)))

# Retrieve data from Redis and convert to DataFrame
nation_df = pd.read_json(redis_client.get('nation'))
supplier_df = pd.read_json(redis_client.get('supplier'))
orders_df = pd.read_json(redis_client.get('orders'))

# Merge the DataFrames (you may need to change the join conditions based on keys)
result_df = pd.merge(mysql_df, mongo_df, how='inner', left_on='l_partkey', right_on='P_PARTKEY')

# Continue with merging or processing other tables if necessary
# ...

# Write the output to a CSV file
result_df.to_csv('query_output.csv', index=False)
