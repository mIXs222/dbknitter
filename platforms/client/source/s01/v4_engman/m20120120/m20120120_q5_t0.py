import pymysql
import pymongo
import pandas as pd
from pandas import DataFrame
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# SQL Query to retrieve ASIAN nations and region in MySQL
sql_query = """
SELECT n.N_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as REVENUE
FROM nation n
JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY AND r.R_NAME = 'ASIA'
JOIN supplier s ON s.S_NATIONKEY = n.N_NATIONKEY
JOIN lineitem l ON l.L_SUPPKEY = s.S_SUPPKEY
JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY AND c.C_NATIONKEY = n.N_NATIONKEY
WHERE l.L_SHIPDATE >= '1990-01-01' AND l.L_SHIPDATE < '1995-01-01'
GROUP BY n.N_NAME
ORDER BY REVENUE DESC;
"""

# Execute the query on MySQL
mysql_cursor.execute(sql_query)
result = mysql_cursor.fetchall()

# Place data into DataFrame
df = pd.DataFrame(result, columns=['N_NAME', 'REVENUE'])

# Save the DataFrame to CSV
df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
redis_client.close()
