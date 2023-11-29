import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Specify the part dimension to filter on
specified_dim = 'YOUR_DIMENSION_HERE'  # Replace with actual dimension

mysql_query = f"""
SELECT n.N_NAME as nation, YEAR(o.O_ORDERDATE) as o_year,
       SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) as profit
FROM nation n
JOIN supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
JOIN orders o ON s.S_SUPPKEY = o.O_CUSTKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN partsupp ps ON l.L_PARTKEY = ps.PS_PARTKEY AND l.L_SUPPKEY = ps.PS_SUPPKEY
WHERE l.L_COMMENT LIKE '%{specified_dim}%'
GROUP BY nation, o_year
ORDER BY nation ASC, o_year DESC;
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()
mysql_conn.close()

# Convert MySQL query results to a DataFrame
mysql_df = pd.DataFrame(mysql_results, columns=['nation', 'o_year', 'profit'])

# MongoDB connection and data retrieval for "partsupp" collection
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]
partsupp_collection = mongodb_db["partsupp"]

# Add the PS_SUPPLYCOST and PS_COMMENT from MongoDB to the DataFrame
# This step is placeholder, as the actual merge will depend on the structure of the data
partsupp_df = pd.DataFrame(list(partsupp_collection.find()))
# Here we would perform necessary transformations and merges

# Redis connection and data retrieval for "part" key
redis_conn = DirectRedis(host='redis', port=6379, db=0)
parts_data = redis_conn.get('part')
parts_df = pd.read_json(parts_data)
# DataFrame transformations and filters would go here

# Combine results from each database and save to CSV file
# Assuming necessary transformations and merges were completed

combined_df = mysql_df  # Placeholder for actual combined DataFrame
combined_df.to_csv('query_output.csv', index=False)
