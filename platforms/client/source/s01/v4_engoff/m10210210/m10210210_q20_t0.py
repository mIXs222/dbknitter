# Import required libraries
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL server
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB server
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for relevant data
with mysql_conn.cursor() as cursor:
    cursor.execute("""
    SELECT ps.PS_SUPPKEY, ps.PS_PARTKEY, SUM(l.L_QUANTITY) as total_quantity
    FROM partsupp AS ps
    JOIN lineitem AS l ON ps.PS_PARTKEY = l.L_PARTKEY AND ps.PS_SUPPKEY = l.L_SUPPKEY
    WHERE l.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
    GROUP BY ps.PS_SUPPKEY, ps.PS_PARTKEY
    HAVING total_quantity > 0.5 * SUM(l.L_QUANTITY) OVER (PARTITION BY ps.PS_SUPPKEY)
    """)
    mysql_data = cursor.fetchall()

# Convert query result to DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=['PS_SUPPKEY', 'PS_PARTKEY', 'TOTAL_QUANTITY'])
mysql_conn.close()

# Query MongoDB for relevant data
nation_data = mongo_db.nation.find({"N_NAME": "CANADA"}, {"N_NATIONKEY": 1})
supplier_data = mongo_db.supplier.find({"S_NATIONKEY": {"$in": [doc['N_NATIONKEY'] for doc in nation_data]}}, {"S_SUPPKEY": 1})

# Transform MongoDB data into DataFrame
supplier_df = pd.DataFrame(list(supplier_data))

# Query Redis for relevant data
part_keys = redis_conn.keys('part:*')
part_data = [redis_conn.hgetall(key) for key in part_keys]

# Convert Redis data into DataFrame
part_df = pd.DataFrame(part_data)

# Merge data from all databases
merged_df = (
    mysql_df
    .merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY', how='inner')
    .merge(part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY', how='inner')
)

# Filter for parts with names that share a certain naming convention for forest parts
filtered_df = merged_df[merged_df['P_NAME'].str.contains('forest', case=False)]

# Save the results to csv file
filtered_df.to_csv('query_output.csv', index=False)
