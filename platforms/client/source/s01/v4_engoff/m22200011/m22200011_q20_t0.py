# Python Code (retrieve_data.py)

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Configuration for connecting to the MySQL database
mysql_config = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch",
}

# Configuration for connecting to the MongoDB database
mongodb_config = {
    "host": "mongodb",
    "port": 27017,
    "database": "tpch",
}

# Configuration for connecting to the Redis database
redis_config = {
    "host": "redis",
    "port": 6379,
    "db": 0,
}

# Connect to MySQL
mysql_db = pymysql.connect(**mysql_config)
mysql_cursor = mysql_db.cursor()

# Query for MySQL
mysql_query = """
SELECT S.S_NAME,
       S.S_ADDRESS,
       SUM(PS.PS_AVAILQTY) AS TOTAL_AVAIL_QTY
FROM supplier AS S JOIN partsupp AS PS ON S.S_SUPPKEY = PS.PS_SUPPKEY
GROUP BY S.S_SUPPKEY
HAVING TOTAL_AVAIL_QTY > (SELECT 0.5 * SUM(L.L_QUANTITY)
                           FROM lineitem AS L
                           WHERE L.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
                             AND L.L_SUPPKEY = S.S_SUPPKEY)
"""
mysql_cursor.execute(mysql_query)
suppliers_with_excess = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_NAME', 'S_ADDRESS', 'TOTAL_AVAIL_QTY'])

mysql_db.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(**mongodb_config)
mongodb = mongo_client[mongodb_config['database']]

# Query for MongoDB
lineitem_data = list(mongodb.lineitem.find({
    "L_SHIPDATE": {
        "$gte": datetime(1994, 1, 1),
        "$lte": datetime(1995, 1, 1),
    },
}, {
    "L_PARTKEY": 1,
    "L_SUPPKEY": 1,
    "L_QUANTITY": 1,
}))
lineitem_df = pd.DataFrame(lineitem_data)

# Connect to Redis
redis_client = DirectRedis(**redis_config)

# Retrieve data from Redis
nation_df = pd.read_json(redis_client.get('nation'))
part_df = pd.read_json(redis_client.get('part'))

# Filter parts with naming convention and find parts for CANADA
canada_nations = nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'].tolist()
parts_with_naming_convention = part_df[part_df['P_NAME'].str.contains('forest', case=False)]

# Combine the results
combined_df = pd.merge(lineitem_df, parts_with_naming_convention, left_on='L_PARTKEY', right_on='P_PARTKEY', how='inner')
combined_df = combined_df[combined_df['L_SUPPKEY'].isin(canada_nations)]

# Write the final result to a CSV file
final_df = pd.merge(combined_df, suppliers_with_excess, left_on='L_SUPPKEY', right_on='S_NAME', how='inner')
final_df.to_csv('query_output.csv', index=False)
