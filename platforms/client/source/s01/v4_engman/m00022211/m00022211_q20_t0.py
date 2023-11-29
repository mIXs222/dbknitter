# query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Define the SQL query to fetch relevant data from MySQL (parts matching the forest naming convention)
mysql_query = """
SELECT p.P_PARTKEY, s.S_SUPPKEY
FROM part AS p
JOIN lineitem AS l ON p.P_PARTKEY = l.L_PARTKEY
JOIN supplier AS s ON l.L_SUPPKEY = s.S_SUPPKEY
JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE p.P_NAME LIKE '%forest%'
AND l.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
AND n.N_NAME = 'CANADA';
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# convert mysql results into a pandas DataFrame
mysql_df = pd.DataFrame(mysql_results, columns=['P_PARTKEY', 'S_SUPPKEY'])

# MongoDB connection and query
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

# Getting the documents from MongoDB and filtering in Python (since the query is across different platforms)
mongodb_results = lineitem_collection.find({
    'L_SHIPDATE': {'$gte': '1994-01-01', '$lte': '1995-01-01'}
})
mongodb_df = pd.DataFrame(list(mongodb_results))

# Redis connection and data fetching
redis_conn = DirectRedis(host='redis', port=6379, db=0)
supplier_redis = pd.read_json(redis_conn.get('supplier'), dtype={'S_SUPPKEY': int})
partsupp_redis = pd.read_json(redis_conn.get('partsupp'), dtype={'PS_PARTKEY': int, 'PS_SUPPKEY': int})

# Join the data from all sources
final_df = pd.merge(mysql_df, mongodb_df, how='inner', left_on=['P_PARTKEY', 'S_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
final_df = pd.merge(final_df, supplier_redis, how='inner', left_on='S_SUPPKEY', right_on='S_SUPPKEY')
final_df = pd.merge(final_df, partsupp_redis, how='inner', left_on=['P_PARTKEY', 'S_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])

# Assuming 'forest part' means the P_NAME including 'forest', now filter the parts with excess quantity
final_df['excess'] = final_df['PS_AVAILQTY'] > (final_df['L_QUANTITY'] * 0.5)
supplier_excess = final_df[final_df['excess']].groupby('S_SUPPKEY').size().reset_index(name='counts')

# Write the query output to a CSV file
supplier_excess.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongodb_client.close()
redis_conn.close()
